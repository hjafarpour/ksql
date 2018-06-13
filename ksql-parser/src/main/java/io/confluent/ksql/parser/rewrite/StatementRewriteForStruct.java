/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.rewrite;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import org.apache.kafka.connect.data.Field;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.util.DataSourceExtractor;

public class StatementRewriteForStruct {

  private Statement statement;
  private MetaStore metaStore;
  private DataSourceExtractor dataSourceExtractor;

  public StatementRewriteForStruct(
      final Statement statement,
      final MetaStore metaStore,
      final DataSourceExtractor dataSourceExtractor
  ) {
    this.statement = statement;
    this.metaStore = metaStore;
    this.dataSourceExtractor = dataSourceExtractor;
  }

  public Statement rewriteForStruct() {
    RewriteWithStructFieldExtractors statementRewriter = new RewriteWithStructFieldExtractors();
    return (Statement) statementRewriter.process(statement, null);
  }


  private class RewriteWithStructFieldExtractors extends StatementRewriter {

    @Override
    protected Node visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      return createFetchFunctionNodeIfNeeded(node, context);
    }

    private Expression createFetchFunctionNodeIfNeeded(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      if (dereferenceExpression.getBase() instanceof QualifiedNameReference) {
        String sourceName = dereferenceExpression.getBase().toString();
        if (dataSourceExtractor.getAliasToNameMap().containsKey(sourceName)) {
          sourceName = dataSourceExtractor.getAliasToNameMap().get(sourceName);
        }
        StructuredDataSource structuredDataSource = metaStore.getSource(sourceName);
        Field field = structuredDataSource.getSchema().field(
            dereferenceExpression.getFieldName().toUpperCase()
        );
        DereferenceExpression newDereferenceExpression;
        if (dereferenceExpression.getLocation().isPresent()) {
          newDereferenceExpression = new DereferenceExpression(
              dereferenceExpression.getLocation().get(),
              (Expression) process(dereferenceExpression.getBase(), context),
              dereferenceExpression.getFieldName()
          );
        } else {
          newDereferenceExpression = new DereferenceExpression(
              (Expression) process(dereferenceExpression.getBase(), context),
              dereferenceExpression.getFieldName()
          );
        }

        return newDereferenceExpression;
      } else if (dereferenceExpression.getBase() instanceof SubscriptExpression) {
        return new FunctionCall(
            QualifiedName.of("FETCH_FIELD_FROM_STRUCT"),
            ImmutableList.of(
                dereferenceExpression.getBase(),
                new StringLiteral(dereferenceExpression.getFieldName())
            ));
      }
      List<Expression> argList = new ArrayList<>();
      Expression createFunctionResult = createFetchFunctionNodeIfNeeded(
          (DereferenceExpression) dereferenceExpression.getBase(), context);

      argList.add(createFunctionResult);
      String fieldName = dereferenceExpression.getFieldName();
      argList.add(new StringLiteral(fieldName));
      return new FunctionCall(QualifiedName.of("FETCH_FIELD_FROM_STRUCT"), argList);
    }
  }

}
