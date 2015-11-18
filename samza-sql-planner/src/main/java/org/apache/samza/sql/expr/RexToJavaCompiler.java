/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.expr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.samza.SamzaException;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.*;
import java.util.List;

/**
 * Defines a SQL row expression to a java class ({@link org.apache.samza.sql.api.expressions.Expression}) compiler.
 *
 * <p>This is based on Calcite's {@link org.apache.calcite.interpreter.JaninoRexCompiler}. This first generates
 * a Java AST and them compile it to a class using Janino.</p>
 */
public class RexToJavaCompiler {
  private static final Logger log = LoggerFactory.getLogger(RexToJavaCompiler.class);

  private final RexBuilder rexBuilder;

  public RexToJavaCompiler(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }


  /**
   * Compiles a row expression to a instance of {@link org.apache.samza.sql.expr.Expression}
   * @param inputs Input relations/time-varying relations for this row expression
   * @param nodes row expression
   * @return compiled expression of type {@link org.apache.samza.sql.expr.Expression}
   */
  public org.apache.samza.sql.expr.Expression compile(List<RelNode> inputs, List<RexNode> nodes) {
    /*
       In case there are multiple input relations, we build a single input row type combining types of all the inputs.
     */
    final RelDataTypeFactory.FieldInfoBuilder fieldBuilder =
        rexBuilder.getTypeFactory().builder();
    for (RelNode input : inputs) {
      fieldBuilder.addAll(input.getRowType().getFieldList());
    }
    final RelDataType inputRowType = fieldBuilder.build();
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    for (RexNode node : nodes) {
      programBuilder.addProject(node, null);
    }
    final RexProgram program = programBuilder.getProgram();

    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression inputValues =
        Expressions.parameter(Object[].class, "inputValues");
    final ParameterExpression outputValues =
        Expressions.parameter(Object[].class, "outputValues");
    final JavaTypeFactoryImpl javaTypeFactory =
        new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());

    // public void execute(Object[] inputValues, Object[] outputValues)
    final RexToLixTranslator.InputGetter inputGetter =
        new RexToLixTranslator.InputGetterImpl(
            ImmutableList.of(
                Pair.<org.apache.calcite.linq4j.tree.Expression, PhysType>of(
                    Expressions.variable(Object[].class, "inputValues"),
                    PhysTypeImpl.of(javaTypeFactory, inputRowType,
                        JavaRowFormat.ARRAY, false))));
    final Function1<String, RexToLixTranslator.InputGetter> correlates =
        new Function1<String, RexToLixTranslator.InputGetter>() {
          public RexToLixTranslator.InputGetter apply(String a0) {
            throw new UnsupportedOperationException();
          }
        };
    final List<org.apache.calcite.linq4j.tree.Expression> list =
        RexToLixTranslator.translateProjects(program, javaTypeFactory, builder,
            null, null, inputGetter, correlates);
    for (int i = 0; i < list.size(); i++) {
      builder.add(
          Expressions.statement(
              Expressions.assign(
                  Expressions.arrayIndex(outputValues,
                      Expressions.constant(i)),
                  list.get(i))));
    }
    return baz(inputValues, outputValues, builder.toBlock());
  }

  /**
   * Given a method that implements {@link org.apache.samza.sql.expr.Expression#execute(Object[], Object[])},
   * adds a bridge method that implements {@link org.apache.samza.sql.expr.Expression#execute(Object[])}, and
   * compiles.
   */
  static org.apache.samza.sql.expr.Expression baz(ParameterExpression inputValues,
                                                             ParameterExpression outputValues, BlockStatement block) {
    final List<MemberDeclaration> declarations = Lists.newArrayList();

    // public void execute(Object[] inputValues, Object[] outputValues)
    declarations.add(
        Expressions.methodDecl(Modifier.PUBLIC, void.class,
            SamzaBuiltInMethod.EXPR_EXECUTE2.method.getName(),
            ImmutableList.of(inputValues, outputValues), block));

    // public Object execute(Object[] inputValues)
    final BlockBuilder builder = getExecuteMethodWithOnlyInputsBlockBuilder(inputValues);

    declarations.add(
        Expressions.methodDecl(Modifier.PUBLIC, Object.class,
            SamzaBuiltInMethod.EXPR_EXECUTE1.method.getName(),
            ImmutableList.of(inputValues), builder.toBlock()));

    final ClassDeclaration classDeclaration =
        Expressions.classDecl(Modifier.PUBLIC, "SqlExpression", null,
            ImmutableList.<Type>of(org.apache.samza.sql.expr.Expression.class), declarations);
    String s = Expressions.toString(declarations, "\n", false);

    if (log.isDebugEnabled()) {
      log.debug("Generated code for expression: " + s);
    }

    try {
      return getExpression(classDeclaration, s);
    } catch (Exception e) {
      throw new SamzaException("Expression compilation failure.", e);
    }
  }

  static BlockBuilder getExecuteMethodWithOnlyInputsBlockBuilder(ParameterExpression inputValues){
    final BlockBuilder builder = new BlockBuilder();
    final Expression values = builder.append("values",
        Expressions.newArrayBounds(Object.class, 1,
            Expressions.constant(1)));
    builder.add(
        Expressions.statement(
            Expressions.call(
                Expressions.parameter(Scalar.class, "this"),
                SamzaBuiltInMethod.EXPR_EXECUTE2.method, inputValues, values)));
    builder.add(
        Expressions.return_(null,
            Expressions.arrayIndex(values, Expressions.constant(0))));

    return builder;
  }

  static org.apache.samza.sql.expr.Expression getExpression(ClassDeclaration expr, String s)
      throws CompileException, IOException {
    ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setImplementedInterfaces(expr.implemented.toArray(new Class[expr.implemented.size()]));
    cbe.setParentClassLoader(RexToJavaCompiler.class.getClassLoader());
    cbe.setDebuggingInformation(true, true, true);

    System.out.println(s);

    return (org.apache.samza.sql.expr.Expression) cbe.createInstance(new StringReader(s));
  }

  public enum SamzaBuiltInMethod {
    EXPR_EXECUTE1(org.apache.samza.sql.expr.Expression.class, "execute", Object[].class),
    EXPR_EXECUTE2(org.apache.samza.sql.expr.Expression.class, "execute", Object[].class, Object[].class);

    public final Method method;
    public final Constructor constructor;
    public final Field field;

    public static final ImmutableMap<java.lang.reflect.Method, BuiltInMethod> MAP;

    static {
      final ImmutableMap.Builder<Method, BuiltInMethod> builder =
          ImmutableMap.builder();
      for (BuiltInMethod value : BuiltInMethod.values()) {
        if (value.method != null) {
          builder.put(value.method, value);
        }
      }
      MAP = builder.build();
    }

    private SamzaBuiltInMethod(Method method, Constructor constructor, Field field) {
      this.method = method;
      this.constructor = constructor;
      this.field = field;
    }

    /**
     * Defines a method.
     */
    SamzaBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
      this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /**
     * Defines a constructor.
     */
    SamzaBuiltInMethod(Class clazz, Class... argumentTypes) {
      this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /**
     * Defines a field.
     */
    SamzaBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
      this(null, null, Types.lookupField(clazz, fieldName));
      assert dummy : "dummy value for method overloading must be true";
    }
  }

}
