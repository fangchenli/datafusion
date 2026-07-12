// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::logical_plan::producer::{SubstraitProducer, to_substrait_type};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::{Expr, ExprSchemable, expr};
use substrait::proto::aggregate_function::AggregationInvocation;
use substrait::proto::aggregate_rel::Measure;
use substrait::proto::function_argument::ArgType;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::{
    AggregateFunction, AggregationPhase, FunctionArgument, SortField,
};

pub fn from_aggregate_function(
    producer: &mut impl SubstraitProducer,
    agg_fn: &expr::AggregateFunction,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Measure> {
    let expr::AggregateFunction {
        func,
        params:
            AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment: _null_treatment,
            },
    } = agg_fn;
    let sorts = order_by
        .iter()
        .map(|expr| to_substrait_sort_field(producer, expr, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    let (_, output_field) = Expr::AggregateFunction(agg_fn.clone()).to_field(schema)?;
    let output_type = to_substrait_type(
        producer,
        output_field.data_type(),
        output_field.is_nullable(),
    )?;
    let function_anchor = producer.register_function(func.name().to_string());
    #[expect(deprecated)]
    Ok(Measure {
        measure: Some(AggregateFunction {
            function_reference: function_anchor,
            arguments,
            sorts,
            output_type: Some(output_type),
            invocation: match distinct {
                true => AggregationInvocation::Distinct as i32,
                false => AggregationInvocation::All as i32,
            },
            // A logical-plan aggregate is a complete, single-stage aggregation,
            // which Substrait models as INITIAL_TO_RESULT. Leaving the phase
            // unspecified makes the Measure unconsumable by strict consumers.
            phase: AggregationPhase::InitialToResult as i32,
            args: vec![],
            options: vec![],
        }),
        filter: match filter {
            Some(f) => Some(producer.handle_expr(f, schema)?),
            None => None,
        },
    })
}

/// Converts sort expression to corresponding substrait `SortField`
fn to_substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &expr::Sort,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<SortField> {
    let sort_kind = match (sort.asc, sort.nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(producer.handle_expr(&sort.expr, schema)?),
        sort_kind: Some(SortKind::Direction(sort_kind.into())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::producer::DefaultSubstraitProducer;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::functions_aggregate::expr_fn::sum;
    use datafusion::logical_expr::col;
    use std::sync::Arc;
    use substrait::proto::r#type::Kind;

    // Regression test: the producer must emit `output_type` and a concrete
    // `phase` on aggregate Measures. Omitting either makes the plan
    // unconsumable by strict Substrait consumers (e.g. Acero). Sibling of the
    // scalar-expression fix in #20597.
    #[tokio::test]
    async fn aggregate_output_type_and_phase() -> datafusion::common::Result<()> {
        let state = SessionStateBuilder::default().build();
        let schema: DFSchemaRef = Arc::new(
            Schema::new(vec![Field::new("v", DataType::Int64, true)]).try_into()?,
        );
        let mut producer = DefaultSubstraitProducer::new(&state);

        let Expr::AggregateFunction(agg) = sum(col("v")) else {
            panic!("expected an aggregate function");
        };
        let measure = from_aggregate_function(&mut producer, &agg, &schema)?;
        let agg_fn = measure.measure.expect("measure should be set");

        let output_type = agg_fn
            .output_type
            .expect("aggregate output_type should be set");
        assert!(matches!(output_type.kind, Some(Kind::I64(_))));
        assert_eq!(agg_fn.phase, AggregationPhase::InitialToResult as i32);
        Ok(())
    }
}
