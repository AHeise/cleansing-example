package eu.stratosphere.sopremo.cleansing.examples;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;

@InputCardinality(1)
@Name(verb = "unroll")
public class Unroll extends ElementaryOperator<Unroll> {
	ConstantExpression fieldAccess = new ConstantExpression(
			TextNode.valueOf(""));

	@Property
	public void setOn(EvaluationExpression expr) {
		if (!(expr instanceof ConstantExpression)
				|| !(expr.evaluate(NullNode.getInstance()) instanceof TextNode)) {
			System.out.println("Wrong type of 'on' property.");
		}
		this.fieldAccess = (ConstantExpression) expr;
	}

	public static class Implementation extends SopremoMap {

		private ConstantExpression fieldAccess;

		@SuppressWarnings("unchecked")
		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			String fieldName = ((TextNode) this.fieldAccess.evaluate(NullNode
					.getInstance()))/* getTextValue() */.toString();
			IJsonNode array = ((IObjectNode) value).get(fieldName);

			if (!(IArrayNode.class.isAssignableFrom(array.getClass()))) {
				out.collect(value);
			} else {
				((IObjectNode) value).remove(fieldName);
				if (((IArrayNode<IJsonNode>) array).isEmpty()) {
					((IObjectNode) value)
							.put(fieldName, NullNode.getInstance());
					out.collect(value);
				} else {
					for (IJsonNode node : (IArrayNode<IJsonNode>) array) {
						IObjectNode result = ((IObjectNode) value.clone()).put(
								fieldName, node);
						out.collect(result);
					}
				}
			}
		}
	}
}
