package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import java.util.Arrays;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;

import index.bplusTree.LeafNode;
import manager.StorageManager;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {

    private final List<RexNode> projects;
    private final RelDataType rowType;
    private final RelOptTable table;
    private final RexNode filter;

    public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter,
            List<RexNode> projects) {
        super(cluster, traitSet, table);
        this.table = table;
        this.rowType = deriveRowType();
        this.filter = filter;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PIndexScan(getCluster(), traitSet, table, filter, projects);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PIndexScan";
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }

    @Override
    public List<Object[]> evaluate(StorageManager storage_manager) {
        String tableName = getTableName();
        System.out.println("Evaluating PIndexScan for table: " + tableName);

        /* Write your code here */
        RexCall call = (RexCall) filter;
        SqlOperator operator = call.getOperator();

        // Assuming it's a binary operator

            RexNode left = call.getOperands().get(0);
            RexNode right = call.getOperands().get(1);

            // Get column index and key value
            int columnIndex = -1;
            // Object keyValue = null;
            RexLiteral keyvalue1 = null;
            Object keyValue = null;

            // System.out.println("printing left and rifht "  +left + " "  + right);

            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                columnIndex = ((RexInputRef) left).getIndex();
                keyvalue1 = ((RexLiteral) right);
            } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
                columnIndex = ((RexInputRef) right).getIndex();
                keyvalue1 = ((RexLiteral) left);
            }

            // Get column name
            String columnName = null;
            String dataTypeString = null;
            if (columnIndex != -1) {
                columnName = rowType.getFieldList().get(columnIndex).getName();
                RelDataType dataType = rowType.getFieldList().get(columnIndex).getType();
                dataTypeString = dataType.toString();
            }

            columnIndex = storage_manager.findColumnIndex(tableName, columnName);
            int datatype1 = storage_manager.findColumnDatatype(tableName, columnName);

            if (datatype1 == 1) {
                dataTypeString = "INTEGER";
                keyValue = keyvalue1.getValueAs(Integer.class);
            }
            else if (datatype1 == 2) {
                dataTypeString = "BOOLEAN";
                keyValue = keyvalue1.getValueAs(Boolean.class);
            }
            else if (datatype1 == 3) {
                dataTypeString = "FLOAT";
                keyValue = keyvalue1.getValueAs(Float.class);
            }
            else if (datatype1 == 4) {
                dataTypeString = "DOUBLE";
                // System.out.println("aa gyaaa idhaarrrr");
                keyValue = keyvalue1.getValueAs(Double.class);
            }
            else  {
                dataTypeString = "STRING";
                keyValue = keyvalue1.getValueAs(String.class);
            }


            // if (keyvalue1.getType().getSqlTypeName().getName().equals("INTEGER")) {
            //     keyValue = keyvalue1.getValueAs(Integer.class);
            // }
            // else if (keyvalue1.getType().getSqlTypeName().getName().equals("BOOLEAN")) {
            //     keyValue = keyvalue1.getValueAs(Boolean.class);
            // }
            // else if (keyvalue1.getType().getSqlTypeName().getName().equals("FLOAT")) {
            //     keyValue = keyvalue1.getValueAs(Float.class);
            // }
            // else if (keyvalue1.getType().getSqlTypeName().getName().equals("DOUBLE")) {
            //     keyValue = keyvalue1.getValueAs(Double.class);
            // }
            // else  {
            //     keyValue = keyvalue1.getValueAs(String.class);
            // }

            // System.out.println("printing the column index found " + columnIndex);

            int search_result = storage_manager.search(tableName, columnName, keyvalue1, datatype1);
            // System.out.println("came putttttttt");
            int datatype;
            String operation = operator.getName();
            byte[] keyInByteArray = null;
            String index_file_name = tableName + "_" + columnName + "_index";

            // System.out.println("value od search key of storage manager search is " + search_result);

            if (keyValue instanceof Integer) {
                ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                buffer.putInt((Integer) keyValue);
                keyInByteArray = buffer.array();
            } else if (keyValue instanceof String) {
                // System.out.println("ayaaa hooon idhaarrrr");
                keyInByteArray = ((String) keyValue).getBytes(StandardCharsets.UTF_8);
            } else if (keyValue instanceof Float) {
                ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
                buffer.putFloat((Float) keyValue);
                keyInByteArray = buffer.array();
            } else if (keyValue instanceof Double) {
                ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
                buffer.putDouble((Double) keyValue);
                keyInByteArray = buffer.array();
            } else if (keyValue instanceof Boolean) {
                keyInByteArray = new byte[] { (byte) (((Boolean) keyValue) ? 1 : 0) };
            }


            if (operation.equals("<=")){
                operation = "LE";
            }
            else if (operation.equals(">=")){
                operation = "GE";
            }
            else if (operation.equals("<")){
                operation = "L";
            }
            else if (operation.equals(">")){
                operation = "G";
            }
            else if (operation.equals("=")){
                operation = "E";
            }

            Set<Integer> set_of_block_ids_in_db = new HashSet<>();;
            if (dataTypeString.equals("INTEGER")){
                datatype = 1;
                set_of_block_ids_in_db = getBlocksidsforleaf(index_file_name, search_result, storage_manager, datatype, keyInByteArray, operation);
            }
            else if (dataTypeString.equals("STRING")){
                datatype = 0;
                set_of_block_ids_in_db = getBlocksidsforleaf(index_file_name, search_result, storage_manager, datatype, keyInByteArray, operation);
            }
            else if (dataTypeString.equals("FLOAT")){
                datatype = 3;
                set_of_block_ids_in_db = getBlocksidsforleaf(index_file_name, search_result, storage_manager, datatype, keyInByteArray, operation);
            }
            else if (dataTypeString.equals("DOUBLE")){
                datatype = 4;
                set_of_block_ids_in_db = getBlocksidsforleaf(index_file_name, search_result, storage_manager, datatype, keyInByteArray, operation);
            }
            else if (dataTypeString.equals("BOOLEAN")){
                datatype = 2;
                set_of_block_ids_in_db = getBlocksidsforleaf(index_file_name, search_result, storage_manager, datatype, keyInByteArray, operation);
            }

            List<Object[]> relevantRecordsArray = new ArrayList<>();

            Iterator<Integer> iterator = set_of_block_ids_in_db.iterator();
            while (iterator.hasNext()) {
                int element = iterator.next();
                List<Object[]> allRecordsArray = storage_manager.get_records_from_block(tableName, element);
                // System.out.println("element " + element + " length of array is " + allRecordsArray.size());
                for (int i = 0; i < allRecordsArray.size(); i++) {
                    Object key = allRecordsArray.get(i)[columnIndex]; // Retrieve the key

                    if ("E".equals(operation)) {
                        if (key.equals(keyValue)) {
                            relevantRecordsArray.add(allRecordsArray.get(i));
                        }
                    } else if ("GE".equals(operation)) {
                        if (compareValues(key, keyValue) >= 0) {
                            relevantRecordsArray.add(allRecordsArray.get(i));
                        }
                    } else if ("LE".equals(operation)) {
                        if (compareValues(key, keyValue) <= 0) {
                            relevantRecordsArray.add(allRecordsArray.get(i));
                        }
                    } else if ("G".equals(operation)) {
                        if (compareValues(key, keyValue) > 0) {
                            relevantRecordsArray.add(allRecordsArray.get(i));
                        }
                    } else if ("L".equals(operation)) {
                        if (compareValues(key, keyValue) < 0) {
                            relevantRecordsArray.add(allRecordsArray.get(i));
                        }
                    }
                }
            }

            // printed the relevant records here

            //  for (Object[] array : relevantRecordsArray) {
            //      for (Object element : array) {
            //          System.out.print(element + " ");
            //      }
            //      // System.out.println(array[columnIndex]);
            //      System.out.println(); // Print a new line after each array
            //  }

            //  System.out.println("priting the number of relevant records " + relevantRecordsArray.size());

            return relevantRecordsArray;

    }

    // Method to compare values of different data types
    private int compareValues(Object value1, Object value2) {
        if (value1 instanceof Integer && value2 instanceof Integer) {
            return Integer.compare((int) value1, (int) value2);
        } else if (value1 instanceof Float && value2 instanceof Float) {
            return Float.compare((float) value1, (float) value2);
        } else if (value1 instanceof Double && value2 instanceof Double) {
            return Double.compare((double) value1, (double) value2);
        } else if (value1 instanceof String && value2 instanceof String) {
            return ((String) value1).compareTo((String) value2);
        } else if (value1 instanceof Boolean && value2 instanceof Boolean) {
            return Boolean.compare((boolean) value1, (boolean) value2);
        } else {
            // Handle unsupported data types
            throw new IllegalArgumentException("Unsupported data types for comparison: " + value1.getClass() + ", " + value2.getClass());
        }
    }

    public Set<Integer> getBlockIdswithoutt(byte[] datablock){
        int numKeys = (datablock[0] << 8) | (datablock[1] & 0xFF);
        Set<Integer> set = new HashSet<>();

        int cur_pointer = 10;
        for (int i = 0; i < numKeys ; i++){
            int len_key = (datablock[cur_pointer] << 8) | (datablock[cur_pointer + 1] & 0xFF);
            byte[] keys_object = Arrays.copyOfRange(datablock, cur_pointer + 2, cur_pointer + len_key);
            int block_id = (datablock[cur_pointer - 2] << 8) | (datablock[cur_pointer - 1] & 0xFF);
            set.add(block_id);
            cur_pointer += (4 + len_key);
        }
        return set;
    }

    public boolean compareKeys(int dataType, byte[] key1, byte[] key2, String operator) {

        if (dataType == 1) { // Int
            ByteBuffer wrapped1 = ByteBuffer.wrap(key1);
            int k1 = wrapped1.getInt();
            ByteBuffer wrapped2 = ByteBuffer.wrap(key2);
            int k2 = wrapped2.getInt();
            if (operator.equals("E")) {
                return k1 == k2;
            } else if (operator.equals("GE")) {
                return k1 >= k2;
            } else if (operator.equals("G")) {
                return k1 > k2;
            } else if (operator.equals("LE")) {
                return k1 <= k2;
            } else {
                return k1 < k2;
            }
    
        } else if (dataType == 3) { // Float
            ByteBuffer wrapped1 = ByteBuffer.wrap(key1);
            float f1 = wrapped1.getFloat();
            ByteBuffer wrapped2 = ByteBuffer.wrap(key2);
            float f2 = wrapped2.getFloat();
    
            if (operator.equals("E")) {
                return Float.compare(f1, f2) == 0;
            } else if (operator.equals("GE")) {
                return f1 >= f2;
            } else if (operator.equals("G")) {
                return f1 > f2;
            } else if (operator.equals("LE")) {
                return f1 <= f2;
            } else {
                return f1 < f2;
            }

        } else if (dataType == 4) { // Double
            ByteBuffer wrapped1 = ByteBuffer.wrap(key1);
            double d1 = wrapped1.getDouble();
            ByteBuffer wrapped2 = ByteBuffer.wrap(key2);
            double d2 = wrapped2.getDouble();
    
            if (operator.equals("E")) {
                return Double.compare(d1, d2) == 0;
            } else if (operator.equals("GE")) {
                return d1 >= d2;
            } else if (operator.equals("G")) {
                return d1 > d2;
            } else if (operator.equals("LE")) {
                return d1 <= d2;
            } else {
                return d1 < d2;
            }

        } else if (dataType == 0) { // String
            String s1 = new String(key1);
            String s2 = new String(key2);

            if (operator.equals("E")) {
                return s1.compareTo(s2) == 0;
            } else if (operator.equals("GE")) {
                return s1.compareTo(s2) >= 0;
            } else if (operator.equals("G")) {
                return s1.compareTo(s2) > 0;
            } else if (operator.equals("LE")) {
                return s1.compareTo(s2) <= 0;
            } else {
                return s1.compareTo(s2) < 0;
            }

        } else if (dataType == 2) { // Boolean
            boolean b1 = key1[0] != 0;
            boolean b2 = key2[0] != 0;

            if (operator.equals("E")) {
                return b1 == b2;
            }
        }
    
        return false;
    }

    public Set<Integer> getBlocksidsforoneleaf(String file_name, int leaf_id , StorageManager storagemanager, int datatype, byte[] key1, String operation) {
        Set<Integer> answer = new HashSet<>();
        byte[] datablock = storagemanager.get_data_block(file_name, leaf_id);

        int numKeys = (datablock[0] << 8) | (datablock[1] & 0xFF);
        int cur_pointer = 10;
        for (int i = 0; i < numKeys; i++) {
            int len_key = (datablock[cur_pointer] << 8) | (datablock[cur_pointer + 1] & 0xFF);
            byte[] keys_object = new byte[len_key];
            for (int j = 0 ; j < len_key ; j++){
                keys_object[j] = datablock[cur_pointer + 2 + j];
            }
            // byte[] keys_object = Arrays.copyOfRange(datablock, cur_pointer + 2, cur_pointer + len_key);
            int block_id = (datablock[cur_pointer - 2] << 8) | (datablock[cur_pointer - 1] & 0xFF);
            if (compareKeys(datatype, keys_object, key1, operation)) {
                answer.add(block_id);
            }
            cur_pointer += (4 + len_key);
        }
        return answer;
    }

    public Set<Integer> goLeftIds(byte[] datablock, int current_id, String file_name, StorageManager storagemanager){
        // int numKeys = (datablock[0] << 8) | (datablock[1] & 0xFF);
        Set<Integer> set = new HashSet<>();
        set.add(current_id);
        int left_node_pointer = (datablock[2] << 8) | (datablock[3] & 0xFF);

        while (left_node_pointer != -1){
            set.add(left_node_pointer);
            datablock = storagemanager.get_data_block(file_name, left_node_pointer);
            left_node_pointer = (datablock[2] << 8) | (datablock[3] & 0xFF);
        }

        return set;
    }

    public Set<Integer> goRightIds(byte[] datablock, int current_id, String file_name, StorageManager storagemanager){
        // int numKeys = (datablock[0] << 8) | (datablock[1] & 0xFF);
        Set<Integer> set = new HashSet<>();
        set.add(current_id);
        int right_node_pointer = (datablock[4] << 8) | (datablock[5] & 0xFF);
        while (right_node_pointer != -1){
            set.add(right_node_pointer);
            datablock = storagemanager.get_data_block(file_name, right_node_pointer);
            right_node_pointer = (datablock[4] << 8) | (datablock[5] & 0xFF);
        }
        // System.out.println("v kjdsfjadv a");
        return set;
    }

    public Set<Integer> getBlocksidsforleaf(String file_name, int leaf_id , StorageManager storagemanager, int datatype, byte[] key1, String operator) {

        if (leaf_id == -1){
            if (operator.equals("E")) {
                return Collections.emptySet();
            } else if (operator.equals("GE")) {
                return Collections.emptySet();
            } else if (operator.equals("G")) {
                return Collections.emptySet();
            } else if (operator.equals("LE")) {
                // return getBlockIdswithoutt(storagemanager.get_data_block(file_name, leaf_id));
                return getBlocksidsforleaf(file_name, 1 , storagemanager,  datatype, key1, operator);
            } else {
                // return getBlockIdswithoutt(storagemanager.get_data_block(file_name, leaf_id));
                return getBlocksidsforleaf(file_name, 1 , storagemanager,  datatype, key1, operator);
            }
        }
        else{
            boolean go_left = false, go_right = false;
            if (operator.equals("E")) {
                go_right = true;
            } else if (operator.equals("GE")) {
                // System.out.println("cammmeeeee");
                go_right = true;
            } else if (operator.equals("G")) {
                go_right = true;
            } else if (operator.equals("LE")) {
                go_left = true; go_right = true;
            } else {
                go_left = true;
            }

            Set<Integer> set_of_block_ids;
            if (go_left && !go_right){
                set_of_block_ids = goLeftIds(storagemanager.get_data_block(file_name, leaf_id), leaf_id, file_name, storagemanager);
            }
            else if (go_right && !go_left){
                set_of_block_ids = goRightIds(storagemanager.get_data_block(file_name, leaf_id), leaf_id, file_name, storagemanager);
            }
            else{
                set_of_block_ids = goLeftIds(storagemanager.get_data_block(file_name, leaf_id), leaf_id, file_name, storagemanager);
                set_of_block_ids.addAll(goRightIds(storagemanager.get_data_block(file_name, leaf_id), leaf_id, file_name, storagemanager));
            }

            Set<Integer> final_answer_set = new HashSet<>();
            Iterator<Integer> iterator = set_of_block_ids.iterator();
            while (iterator.hasNext()) {
                int element = iterator.next();
                final_answer_set.addAll(getBlocksidsforoneleaf(file_name, element, storagemanager, datatype, key1, operator));
            }

            // System.out.println("printning the elements of the fnal answer set ");
            // for (Integer element : final_answer_set) {
            //     System.out.println(element);
            // }

            return final_answer_set;
        }
    }


}