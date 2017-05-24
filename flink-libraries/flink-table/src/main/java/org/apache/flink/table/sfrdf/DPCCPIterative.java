/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.sfrdf;


import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

@SuppressWarnings({"Duplicates"})
public class DPCCPIterative {

	public DPCCPIterative() {
	}

	public static Map<String, Long> executionTimes = new HashMap<>();
	public static String currentQueryName = "";

	public RelNode optimize(RelNode relNode) {
		System.out.println("STARTING CLIQUE SQUARE OPTIMIZER");
		long startTime = System.currentTimeMillis();

		// Get all projects in the abstract syntax tree
		System.out.println("Getting projects");
		GetProjectsShuttle shuttle = new GetProjectsShuttle();
		relNode.accept(shuttle);
		List<LogicalProject> projects = shuttle.projects;
		System.out.println("Got " + projects.size() + " projects in " + (System.currentTimeMillis() - startTime) + " ms");

		// Remove the first one as it is the top node
		LogicalProject firstProject = projects.remove(0);

		List<Vertex> vertices = new ArrayList<>();

		long start2 = System.currentTimeMillis();
		// Create list of vertices containing variable name info
		for (LogicalProject project : projects) {
			List<String> names = new ArrayList<>();
			for (Pair<RexNode, String> namedProject : project.getNamedProjects()) {
				names.add(namedProject.right);
			}

			vertices.add(new Vertex(names, project));
		}

		// Build join tree
		// A join is present between two projects if they utilize the same variable
		for (int i = 0; i < vertices.size() - 1; i++) {
			Vertex vertex = vertices.get(i);
			for (int j = i + 1; j < vertices.size(); j++) {
				Vertex innerVertex = vertices.get(j);
				vertex.createEdges(innerVertex);
			}
		}
		System.out.println("Done building graph in " + (System.currentTimeMillis() - start2));

		// Do SFRDF bushy plans
		RelNode plan = DPccp(vertices);

		// Put plan back together
		firstProject.replaceInput(0, plan);

		int a = 2;

		executionTimes.put(currentQueryName, System.currentTimeMillis() - startTime);
		return relNode;
	}

	private RelNode DPccp(final List<Vertex> vertices) {
		// Precondition: Do bf search and enumerate vertices
		final List<Vertex> enumeratedVertices = breadthFirstEnumerate(vertices);

		// Table containing current best plan
		Map<Set<Vertex>, RelNode> bestPlan = new HashMap<>();

		// Input base case into table
		for (int i = 0; i < vertices.size(); i++) {
			Vertex relation = vertices.get(i);
			HashSet<Vertex> key = new HashSet<>();
			key.add(relation);
			bestPlan.put(key, relation.project);
		}

		// Find connected subgraphs
		long start = System.currentTimeMillis();
		List<Set<Vertex>> csg = enumerateCsg(enumeratedVertices);
		System.out.println("Enumerate " + csg.size() + " csg's in " + (System.currentTimeMillis() - start));
		System.out.println(csg);

		long start2 = System.currentTimeMillis();
		// Find complements for each one
		List<Tuple2<Set<Vertex>, Set<Vertex>>> csgCmpPairs = new ArrayList<>();
		for (Set<Vertex> set : csg) {
			for (Set<Vertex> complement : enumerateComplements(enumeratedVertices, set)) {
				csgCmpPairs.add(Tuple2.of(set, complement));
			}
		}
		System.out.println("Enumerate " + csgCmpPairs.size() + " csg-cmp-pairs in " + (System.currentTimeMillis() - start2));


		// Foreach pair see if the plan is better than previous ones.
		long start4 = System.currentTimeMillis();
		for (Tuple2<Set<Vertex>, Set<Vertex>> csgCmpPair : csgCmpPairs) {
			RelNode p1 = bestPlan.get(csgCmpPair.f0);
			RelNode p2 = bestPlan.get(csgCmpPair.f1);

			RelNode currentPlan = createJoinTree(p1, p2);

			Set<Vertex> combined = Sets.union(csgCmpPair.f0, csgCmpPair.f1);

			double combinedCost = cost(bestPlan.get(combined));

			if (combinedCost > cost(currentPlan)) {
				bestPlan.put(combined, currentPlan);
			}

			currentPlan = createJoinTree(p2, p1);
			if (combinedCost > cost(currentPlan)) {
				bestPlan.put(combined, currentPlan);
			}
		}
		System.out.println("Checking all pairs in " + (System.currentTimeMillis() - start4));
		return bestPlan.get(new HashSet<>(enumeratedVertices));
	}

	private List<Set<Vertex>> enumerateComplements(List<Vertex> G, Set<Vertex> S1) {
		List<Set<Vertex>> complements = new ArrayList<>();

		int minimumVertex = Collections.min(S1, new Comparator<Vertex>() {
			@Override
			public int compare(Vertex o1, Vertex o2) {
				return o1.id - o2.id;
			}
		}).id;

		Set<Vertex> betaI = new HashSet<>();

		for (int i = 0; i <= minimumVertex; i++) {
			betaI.add(G.get(i));
		}

		Set<Vertex> X = Sets.union(betaI, S1);
		Set<Vertex> N = new HashSet<>();
		for (Vertex vertex : S1) {
			for (Vertex neighbour : vertex.getNeighbours()) {
				if (!X.contains(neighbour)) {
					N.add(neighbour);
				}
			}
		}

		List<Vertex> orderedN = new ArrayList<>(N);

		Collections.sort(orderedN, new Comparator<Vertex>() {
			@Override
			public int compare(Vertex o1, Vertex o2) {
				return o1.id - o2.id;
			}
		});

		for (int i = orderedN.size() - 1; i >= 0; i--) {
			Set<Vertex> set = Sets.newHashSet(orderedN.get(i));
			complements.add(set);

			Queue<Arguments> queue = new LinkedList<>();
			emitCSGs(new ArrayList<Vertex>(), complements, set, betaI);
		}

		return complements;
	}

	private List<Set<Vertex>> enumerateCsg(List<Vertex> enumeratedVertices) {
		// Emitted vertex sets
		final List<Set<Vertex>> connectedSubGraphs = new ArrayList<>();

		// Run through the enumerated vertices in reverse
		for (int i = enumeratedVertices.size() - 1; i >= 0; i--) {
			// Add itself
			final Vertex element = enumeratedVertices.get(i);
			final Set<Vertex> set = new HashSet<>(Arrays.asList(element));
			connectedSubGraphs.add(set);

			// Calculate beta_i, i.e. { v_j | j <= i }
			final Set<Vertex> betaI = new HashSet<>();
			for (int j = 0; j <= i; j++) {
				betaI.add(enumeratedVertices.get(j));
			}

			emitCSGs(enumeratedVertices, connectedSubGraphs, set, betaI);
		}
		return connectedSubGraphs;
	}

	private void emitCSGs(List<Vertex> enumeratedVertices, List<Set<Vertex>> connectedSubGraphs, Set<Vertex> set, Set<Vertex> betaI) {
		final Queue<Arguments> queue = new LinkedList<>();
		queue.add(new Arguments(set, betaI));

		while (!queue.isEmpty()) {
			Arguments current = queue.poll();

			Set<Vertex> neighborhood = new HashSet<>();
			for (Vertex vertex : current.S) {
				for (Vertex neighbour : vertex.getNeighbours()) {
					if (!current.X.contains(neighbour) && !current.S.contains(neighbour)) {
						neighborhood.add(neighbour);
					}
				}
			}

			List<Set<Vertex>> subsets = new ArrayList<>(Sets.powerSet(neighborhood));

			// Order the set by increasing size
			Collections.sort(subsets, new Comparator<Set<Vertex>>() {
				@Override
				public int compare(Set<Vertex> set1, Set<Vertex> set2) {
					return set1.size() - set2.size();
				}
			});

			// Emit subsets
			for (Set<Vertex> subset : subsets) {
				if (subset.size() == 0) {
					continue;
				}
				connectedSubGraphs.add(Sets.union(current.S, subset));
			}

			// Call subsets recursively
			for (Set<Vertex> subset : subsets) {
				Set<Vertex> excludes = Sets.union(current.X, neighborhood);
				if (subset.size() == 0 || excludes.size() == enumeratedVertices.size()) {
					continue;
				}
				queue.add(new Arguments(Sets.union(current.S, subset), excludes));
			}
		}
	}

	private class Arguments {
		public Set<Vertex> S;
		public Set<Vertex> X;

		public Arguments(Set<Vertex> s, Set<Vertex> x) {
			S = s;
			X = x;
		}
	}

	private List<Vertex> breadthFirstEnumerate(List<Vertex> allVertices) {
		Vertex root = allVertices.get(0);

		List<Vertex> workingList = new LinkedList<>();
		List<Vertex> vertices = new ArrayList<>();

		int currentId = 0;

		workingList.add(root);

		while (!workingList.isEmpty()) {
			Vertex current = workingList.remove(0);

			if (current.isVisited()) {
				continue;
			}

			current.id = currentId;
			currentId++;
			vertices.add(current);
			current.setVisited(true);
			workingList.addAll(current.getNeighbours());
		}

		return vertices;
	}


	private double cost(RelNode plan) {
		// TODO: Implement cost function
		if (plan == null) {
			return Double.MAX_VALUE;
		}
		return 0;
	}

	private RelNode createJoinTree(RelNode p1, RelNode p2) {
		RelOptCluster cluster = p1.getCluster();
		RexBuilder rexBuilder = cluster.getRexBuilder();

		List<RelDataTypeField> fieldList1 = p1.getRowType().getFieldList();
		List<RelDataTypeField> fieldList2 = p2.getRowType().getFieldList();

		// TODO: support joins with multiple join conditions

		int joinIndex1 = -1;
		int joinIndex2 = -1;

		for (Ord<RelDataTypeField> field1 : Ord.zip(fieldList1)) {
			for (RelDataTypeField field2 : fieldList2) {
				if (field1.e.getName().equals(field2.getName())) {
					joinIndex1 = field1.i;
					joinIndex2 = fieldList1.size() + fieldList2.indexOf(field2);
					break;
				}
			}
		}

		if (joinIndex1 == -1 || joinIndex2 == -1) {
			System.out.println("FUCK");
		}

		ImmutableList<RelDataTypeField> newFields = ImmutableList.<RelDataTypeField>builder()
			.addAll(fieldList1.iterator())
			.addAll(fieldList2.iterator())
			.build();

		RexNode condition = rexBuilder.makeCall(
			SqlStdOperatorTable.EQUALS,
			RexInputRef.of(joinIndex1, newFields),
			RexInputRef.of(joinIndex2, newFields)
		);

		return LogicalJoin.create(
			p1,
			p2,
			condition,
			ImmutableSet.<CorrelationId>of(),
			JoinRelType.INNER
		);
	}

	private class Vertex {
		private final List<String> variables;
		private final List<Edge> edges;
		private final LogicalProject project;
		private boolean visited = false;
		private int id;

		public Vertex(List<String> variables, LogicalProject project) {
			this.edges = new ArrayList<>();
			this.variables = variables;
			this.project = project;
		}

		public List<Edge> getEdges() {
			return this.edges;
		}

		public List<Vertex> getNeighbours() {
			return Lists.transform(edges, new Function<Edge, DPCCPIterative.Vertex>() {
				@Override
				public DPCCPIterative.Vertex apply(Edge input) {
					return input.vertex2;
				}
			});
		}

		public void addEdge(Edge edge) {
			this.edges.add(edge);
		}

		public void createEdges(Vertex vertex) {
			for (String variable : variables) {
				if (vertex.variables.contains(variable)) {
					Edge edge1 = new Edge(this, vertex, variable);
					Edge edge2 = new Edge(vertex, this, variable);
					this.edges.add(edge1);
					vertex.edges.add(edge2);
				}
			}
		}

		public boolean isVisited() {
			return this.visited;
		}

		public void setVisited(boolean value) {
			this.visited = value;
		}

		@Override
		public boolean equals(Object obj) {
			Vertex that = obj instanceof Vertex ? ((Vertex) obj) : null;
			if (that != null) {
				return this.id == that.id;
			}

			return false;
		}

		@Override
		public String toString() {
			return id + "";
		}
	}

	private class Edge {
		private Vertex vertex1;
		private Vertex vertex2;
		private String variable;

		public Edge(Vertex vertex1, Vertex vertex2, String variable) {
			this.vertex1 = vertex1;
			this.vertex2 = vertex2;
			this.variable = variable;
		}
	}
}
