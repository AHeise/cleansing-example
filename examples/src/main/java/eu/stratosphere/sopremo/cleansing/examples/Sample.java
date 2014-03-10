package eu.stratosphere.sopremo.cleansing.examples;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;

@InputCardinality(1)
@Name(verb = "sample")
public class Sample extends ElementaryOperator<Sample> {

	ConstantExpression selectingExpression = new ConstantExpression(
			IntNode.valueOf(1));

	int counter = 0;

	@Property
	public void setSelectEvery(EvaluationExpression expr) {
		if (!(expr instanceof ConstantExpression)
				|| !(expr.evaluate(NullNode.getInstance()) instanceof IntNode)) {
			System.out.println("Wrong type of 'selectEvery' property.");
		}
		this.selectingExpression = (ConstantExpression) expr;
	}

	public static class Implementation extends SopremoMap {

		private ConstantExpression selectingExpression;

		int counter;

		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			if (this.counter
					% ((IntNode) this.selectingExpression.evaluate(NullNode
							.getInstance())).getIntValue() == 0) {
				out.collect(value);
			}
			this.counter++;
		}
	}

}
