package org.apache.flink.table.sfrdf;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.ArrayList;
import java.util.List;

public class GetProjectsShuttle extends RelShuttleImpl {

	public List<LogicalProject> projects = new ArrayList<>();

	@Override
	public RelNode visit(LogicalProject logicalProject) {
		projects.add(logicalProject);
		return visitChild(logicalProject, 0, logicalProject.getInput());
	}

}
