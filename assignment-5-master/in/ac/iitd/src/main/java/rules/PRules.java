package rules;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import convention.PConvention;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import rel.PProjectFilter;
import rel.PTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);

    private static class PTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PTableScanRule")
                .withRuleFactory(PTableScanRule::new);

        protected PTableScanRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {

            TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();


            if(relOptTable.getRowType() == scan.getRowType()) {
                return PTableScan.create(scan.getCluster(), relOptTable);
            }

            return null;
        }
    }

    // Write a class PProjectFilterRule that converts a LogicalProject followed by a LogicalFilter to a single PProjectFilter node.
    public static class PProjectFilterRule extends RelOptRule {

        public static final PProjectFilterRule INSTANCE = new PProjectFilterRule(
                operand(LogicalProject.class,
                        operand(LogicalFilter.class, any())),
                "ProjectFilterToPProjectFilterRule");

        public PProjectFilterRule(RelOptRuleOperand operand, String description) {
            super(operand, description);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = call.rel(0);
            LogicalFilter filter = call.rel(1);


            List<RexNode> projects = project.getProjects();
            RexNode condition = filter.getCondition();
            RelNode input = filter.getInput();
            // Input is an instance of HelpRelVertex...
            input = ((HepRelVertex) input).getCurrentRel();

            PProjectFilter newRel = PProjectFilter.create(project.getCluster(),
                    input,
                    condition,
                    projects);

            call.transformTo(newRel);
        }
    }
}
