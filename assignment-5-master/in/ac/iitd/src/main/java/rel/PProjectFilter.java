package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;

import convention.PConvention;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
    * PProjectFilter is a relational operator that represents a Project followed by a Filter.
    * You need to write the entire code in this file.
    * To implement PProjectFilter, you can extend either Project or Filter class.
    * Define the constructor accordinly and override the methods as required.
*/
public class PProjectFilter extends Project implements PRel{

    private final List<RexNode> projects;
    private final RexNode condition;

    RelNode input;
    private List<Object[]> data;
    int counter;

    public PProjectFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input,
                          List<RexNode> projects, RexNode condition, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
        this.condition = condition;
        this.projects = projects;
        this.traitSet = this.traitSet.plus(PConvention.INSTANCE);

    }

    public static PProjectFilter create(RelOptCluster cluster, RelNode input,
                                        RexNode condition, List<RexNode> projects) {
        final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(),
                projects, null);

        final RelTraitSet traitSet = input.getTraitSet().replace(PConvention.INSTANCE);
        return new PProjectFilter(cluster, traitSet, input, projects, condition, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PProjectFilter(getCluster(),traitSet,input,projects,condition,rowType);
    }



    public String toString() {
        return "PProjectFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
//        System.out.println("in the open function");
        input = getInput();
//        System.out.println("class of input: " + input.getInputs());
//        System.out.println(condition.toString());
//        System.out.println(projects.toString());
        if(input instanceof PRel){
            ((PRel) input).open();
            data=new ArrayList<>();
            counter=0;
            while (((PRel) input).hasNext()) {
                Object[] row =  ((PRel) input).next();
                data.add(row);
//                System.out.println("row: " + Arrays.toString(row));
            }
//            System.out.println("closing the input stream");
            ((PRel)input).close();
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */

        if (input instanceof PRel) {
            PRel pInput = (PRel) input;
            pInput.close();
        }
        data.clear();
        counter=0;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
        int temp=counter;
        while(temp<data.size()){
            Object[] row = data.get(temp);
            temp++;
            if(evaluateCondition(condition,row)){
//                System.out.println(Arrays.toString(row));
                return true;
            }

        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
        if(hasNext()) {
            while (counter < data.size()) {
                Object[] row = data.get(counter);
                counter++;
                if (evaluateCondition(condition, row)) {
                    Object[] output = new Object[projects.size()];
                    for (int i = 0; i < projects.size(); i++) {
                        output[i] = get_row(projects.get(i), row);
                    }
                    return output;
                }
            }
        }
        return null;
    }



    private boolean evaluateCondition(RexNode condition, Object[] row) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            SqlKind kind = call.getKind();
            List<RexNode> operands = call.getOperands();
            switch (kind) {
                case EQUALS:
                    return evaluateEquals(operands,row);
                case GREATER_THAN:
                    return evaluateGreaterThan(operands, row);
                case GREATER_THAN_OR_EQUAL:
                    return evaluateGreaterThanEqual(operands, row);
                case LESS_THAN:
                    return evaluateLessThan(operands, row);
                case LESS_THAN_OR_EQUAL:
                    return evaluateLessThanEquals(operands, row);
                case AND:
                    int operand_size=operands.size();
                    boolean temp = true;
                    for (int i = 0; i < operand_size; i++) {
                        Comparable value = evaluateCondition(operands.get(i),row);
                        temp = (temp && (boolean)value);
                    }
                    return temp;
                case OR:
                    boolean temp1 = false;
                    for (int i = 0; i < operands.size(); i++) {
                        Comparable value1 = evaluateCondition(operands.get(i),row);
                        temp1 = (temp1 || (boolean)value1);
                    }
                    return temp1;

                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + kind);
            }
        }
        return false;
    }

    private boolean evaluateLessThanEquals(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("Less_than_equals requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)<=0;
    }

    private boolean evaluateGreaterThanEqual(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)>=0;

    }

    private boolean evaluateEquals(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("Equals requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)==0;
    }


    private boolean evaluateGreaterThan(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("GREATER_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)>0;


    }

    private boolean evaluateLessThan(List<RexNode> operands, Object[] row) {
        if (operands.size() != 2) {
            throw new IllegalArgumentException("LESS_THAN requires two operands");
        }
        Comparable value1 = evaluateExpression(operands.get(0), row);
        Comparable value2 = evaluateExpression(operands.get(1), row);
        if(value1 instanceof BigDecimal){
            value1 = convertBigDecimal(value1);
        }
        if(value2 instanceof BigDecimal){
            value2=convertBigDecimal(value2);
        }
        return value1.compareTo(value2)<0;
    }


    private Comparable evaluateExpression(RexNode node, Object[] row) {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            Object value = literal.getValue();
            if (value instanceof BigDecimal) {
                return (BigDecimal) value;
            } else if (value instanceof NlsString) {
                String temp = ((NlsString) value).getValue();
                return temp;
            } else if(value instanceof Boolean) {
                return (Boolean)value;
            }else{
                throw new IllegalArgumentException("Unsupported literal type");
            }
        } else if (node instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) node;
            return (Comparable) row[ref.getIndex()];
        } else if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            if (call.getKind() == SqlKind.PLUS || call.getKind() == SqlKind.MINUS ||
                    call.getKind() == SqlKind.TIMES || call.getKind() == SqlKind.DIVIDE) {
                Comparable left = evaluateExpression(call.getOperands().get(0), row);
                Comparable right = evaluateExpression(call.getOperands().get(1), row);

                if (left instanceof BigDecimal) {
                    left = convertBigDecimal(left);
                }

                if (right instanceof BigDecimal) {
                    right = convertBigDecimal(right);
                }
                if (left instanceof Double && right instanceof Double) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / (Double) right;
                    }
                } else if (left instanceof Float && right instanceof Float) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Float) left + (Float) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Float) left - (Float) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Float) left * (Float) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Float) left / (Float) right;
                    }
                } else if (left instanceof Integer && right instanceof Integer) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Integer) left + (Integer) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Integer) left - (Integer) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Integer) left * (Integer) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Integer) left / (Integer) right;
                    }
                } else if (left instanceof Double && right instanceof Integer) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * ((Integer) right).doubleValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / ((Integer) right).doubleValue();
                    }
                } else if (left instanceof Integer && right instanceof Double) {
                    // Convert left to Double
                    if (call.getKind() == SqlKind.PLUS) {
                        return ((Integer) left).doubleValue() +  (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Integer) left).doubleValue()- (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Integer) left).doubleValue() * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Integer) left).doubleValue() / (Double) right;
                    }
                } else if (left instanceof Float && right instanceof Integer) {
                    // Convert right to Float
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Float) left + ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Float) left - ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Float) left * ((Integer) right).floatValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Float) left / ((Integer) right).floatValue();
                    }
                } else if (left instanceof Integer && right instanceof Float) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return ((Integer) left).floatValue() + (float)right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Integer) left).floatValue() - (float)right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Integer) left).floatValue() * (float)right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Integer) left).floatValue() / (float)right;
                    }
                }else if (left instanceof Float && right instanceof Double) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return  ((Float) left).doubleValue() + (Double) right;
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return ((Float) left).doubleValue() - (Double) right;
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return ((Float) left).doubleValue() * (Double) right;
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return ((Float) left).doubleValue() / (Double) right;
                    }
                } else if (left instanceof Double && right instanceof Float) {
                    if (call.getKind() == SqlKind.PLUS) {
                        return (Double) left + ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.MINUS) {
                        return (Double) left - ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.TIMES) {
                        return (Double) left * ((Float) right).doubleValue();
                    } else if (call.getKind() == SqlKind.DIVIDE) {
                        return (Double) left / ((Float) right).doubleValue();
                    }
                }
                else {
                    throw new IllegalArgumentException("Unsupported types for arithmetic operation");
                }

            }
        }
        throw new UnsupportedOperationException("Unsupported type of expression");
    }


    private Comparable convertBigDecimal(Comparable value) {
        BigDecimal bigDecimalValue = (BigDecimal) value;
        if (bigDecimalValue.scale() <= 0) {
            return bigDecimalValue.intValue();
        } else if (bigDecimalValue.scale() == 1) {
            return bigDecimalValue.floatValue();
        } else {
            return bigDecimalValue.doubleValue();
        }

    }


    private Object get_row(RexNode expression, Object[] inputRow) {
        if (expression instanceof RexInputRef) {
            RexInputRef ref = (RexInputRef) expression;
            return inputRow[ref.getIndex()];
        } else if (expression instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) expression;
            return literal.getValue3();
        } else if (expression instanceof RexCall) {
            RexCall call = (RexCall) expression;
            Object result ;
            List<RexNode> operands = call.getOperands();
            Object a,b;
            switch (call.getKind()) {
                case PLUS:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() + ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() + ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() + ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
                    }
                    break;
                case MINUS:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() - ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() - ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() - ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for sub: " + a + ", " + b);
                    }
                    break;
                case TIMES:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() * ((Number) b).doubleValue();
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() * ((Number) b).floatValue();
                        } else{
                            result = ((Number) a).intValue() * ((Number) b).intValue();
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for mul: " + a + ", " + b);
                    }
                    break;
                case DIVIDE:
                    a = get_row(operands.get(0), inputRow);
                    b = get_row(operands.get(1), inputRow);
                    if (a instanceof Number && b instanceof Number) {
                        double divisor = ((Number) b).doubleValue();
                        if (divisor == 0) throw new ArithmeticException("Division by zero");
                        if (a instanceof Double || b instanceof Double) {
                            result =  ((Number) a).doubleValue() / divisor;
                        } else if (a instanceof Float || b instanceof Float) {
                            result = ((Number) a).floatValue() / (float) divisor;
                        } else{
                            result = ((Number) a).intValue() / (int) divisor;
                        }
                    }else{
                        throw new IllegalArgumentException("Invalid arguments for add: " + a + ", " + b);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + call.getKind());
            }
            return result;
        } else {
            throw new IllegalArgumentException("Unsupported RexNode type: " + expression.getClass());
        }
    }



}
