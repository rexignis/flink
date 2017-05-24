package org.apache.flink.table.sfrdf;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class CostFunction {
	public static double cost(RelNode node) {
		if (node instanceof LogicalTableScan) {
			return cost((LogicalTableScan)node);
		} else if (node instanceof LogicalFilter) {
			return cost((LogicalFilter) node);
		} else if (node instanceof LogicalJoin) {
			return cost((LogicalJoin) node);
		} else {
			throw new NotImplementedException();
		}
	}

	public static double cost(LogicalTableScan node) {
		String tableName = "";// get table name

		// Lookup table cost

		return 0d;
	}

	public static double cost(LogicalFilter node) {

		// check if subject, predicate or object is bound and use according function

		return 0d;
	}

	public static double cost(LogicalJoin node) {
		double joinCost = 0d; // calculate join cost
		return joinCost + Math.max(cost(node.getRight()), cost(node.getLeft()));
	}
}
