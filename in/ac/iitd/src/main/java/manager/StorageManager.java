package manager;

import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;
import index.bplusTree.BPlusTreeIndexFile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    // System.out.println("feddddd  upppppp");
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    // System.out.println("aaya hoooonnnnn");
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {

                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    // if(val.length() == 0) {
                    //     System.out.println("Size = 0");
                    // }
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    private List<Integer> findDataTypes(String file_name) {
        // System.out.println(file_name);
        byte[] schemaBlock = get_data_block(file_name, 0);
        int numCols = ((schemaBlock[1] << 8) | (schemaBlock[0] & 0xFF));  // little endian
        List<Integer> dataTypes = new ArrayList<>(); // Initialize the list
        int cur_pointer = 2;
        int datatype;
        for (int i = 0; i < numCols; i++) {
            int offsetToRead = ((schemaBlock[cur_pointer+1] << 8) | (schemaBlock[cur_pointer] & 0xFF));    // little endian
            datatype = (int) schemaBlock[offsetToRead];
            cur_pointer += 2;
            dataTypes.add(datatype);
        }
        return dataTypes;
    }

    public int findColumnIndex(String file_name, String column_name) {

        byte[] schemaBlock = get_data_block(file_name, 0);
        int numCols = ((schemaBlock[1] << 8) | (schemaBlock[0] & 0xFF));
        int cur_pointer = 2, col_no = 0;
        String col_name ;
        for (int i = 0; i < numCols; i++) {
            int offsetToRead = ((schemaBlock[cur_pointer+1] << 8) | (schemaBlock[cur_pointer ] & 0xFF));
            int lengthtoread = (int) schemaBlock[offsetToRead+1];
            byte[] stringarray = new byte[lengthtoread];
            for (int j = lengthtoread - 1 ; j >= 0 ; j--){                  // check if this is correct wrt big and little endian
                stringarray[j] = schemaBlock[offsetToRead+2+j];
            }
            col_name = new String(stringarray);
            if (col_name.equals(column_name)){
                col_no = i;
                break;
            }
            cur_pointer += 2;
        }
        return col_no;
    }

    public int findColumnDatatype(String file_name, String column_name) {

        byte[] schemaBlock = get_data_block(file_name, 0);
        int numCols = ((schemaBlock[1] << 8) | (schemaBlock[0] & 0xFF));
        int cur_pointer = 2;
        String col_name ;
        int datatype = -1;
        for (int i = 0; i < numCols; i++) {
            int offsetToRead = ((schemaBlock[cur_pointer+1] << 8) | (schemaBlock[cur_pointer ] & 0xFF));
            int lengthtoread = (int) schemaBlock[offsetToRead+1];
            byte[] stringarray = new byte[lengthtoread];
            for (int j = lengthtoread - 1 ; j >= 0 ; j--){                  // check if this is correct wrt big and little endian
                stringarray[j] = schemaBlock[offsetToRead+2+j];
            }
            col_name = new String(stringarray);
            if (col_name.equals(column_name)){
                // col_no = i;
                datatype = (int) schemaBlock[offsetToRead];
                break;
            }
            cur_pointer += 2;
        }
        return datatype;
    }

    private int numberOfFixedLengthRecords(List<Integer> dataTypes){
        int ans = 0;
        for (int i = 0; i < dataTypes.size(); i++){
            if (dataTypes.get(i) != 0){ans += 1;}
            else{break;}
        }
        return ans;
    }

    private int numberOfVariableLengthRecords(List<Integer> dataTypes){
        int ans = 0;
        for (int i = 0; i < dataTypes.size(); i++){
            if (dataTypes.get(i) == 0){ans += 1;}
        }
        return ans;
    }

    private int totalFixedLengthOffset(List<Integer> dataTypes){
        int ans = 0;
        for (int i = 0; i < dataTypes.size(); i++){
            if (dataTypes.get(i) == 1){ans += 4;}
            if (dataTypes.get(i) == 2){ans += 1;}
            if (dataTypes.get(i) == 3){ans += 4;}
            if (dataTypes.get(i) == 4){ans += 8;}
            else{break;}
        }
        return ans;
    }

    private Object[] convertToObjectArray(byte[] row, List<Integer> typeList) {

        int numberOfFixedLengthRecordsans = numberOfFixedLengthRecords((typeList));
        int offsetoffixedvariables = 2*2*numberOfVariableLengthRecords((typeList));
        int offsetnullbitmap = offsetoffixedvariables + totalFixedLengthOffset(typeList);

        List<Object> answer = new ArrayList<>();
        // first reading the fixed length records
        int cur_pointer = 2*2*numberOfVariableLengthRecords((typeList));
        int cur_pointer_null_bitmap = offsetnullbitmap;
        int col_no = 0;

        for (int i = 0 ; i < numberOfFixedLengthRecords(typeList) ; i++){

            int datatype = typeList.get(i);
            // int nullbitmapvalue = ((row[offsetnullbitmap + (col_no/8)] >> (7-(col_no%8))) & 1) ; // check if this is correct
            int nullbitmapvalue = (((row[offsetnullbitmap + (col_no/8)]) & (1 << (7-(col_no%8)))));
            // if (nullbitmapvalue == 1){System.out.println("row[offsetnullbitmap + (col_no/8)] : " + row[offsetnullbitmap + (col_no/8)] + " row[offsetnullbitmap + (col_no/8)] >> (7-(col_no%8)) : " + (row[offsetnullbitmap + (col_no/8)] >> (7-(col_no%8))) ) ;}
            if (datatype == 1){
                if (nullbitmapvalue == 1){answer.add(null);}
                else{
                    int integerDataType = ((row[cur_pointer+3] & 0xFF) << 24) | ((row[cur_pointer + 2] & 0xFF) << 16) | ((row[cur_pointer + 1] & 0xFF) << 8) | (row[cur_pointer + 0] & 0xFF);
                    answer.add(integerDataType);
                }
                cur_pointer += 4;
            }
            else if (datatype == 2){
                if (nullbitmapvalue == 1){answer.add(null);}
                else{
                    boolean booldatatype = (row[cur_pointer] != 0);
                    answer.add(booldatatype);
                }
                cur_pointer += 1;
            }
            else if (datatype == 3){
                if (nullbitmapvalue == 1){answer.add(null);}
                else{
                    int intBits = ((row[cur_pointer + 3] & 0xFF) << 24) | ((row[cur_pointer + 2] & 0xFF) << 16) | ((row[cur_pointer + 1] & 0xFF) << 8) | (row[cur_pointer + 0] & 0xFF);
                    float val = Float.intBitsToFloat(intBits);
                    answer.add(val);
                }
                cur_pointer += 4;
            }
            else if (datatype == 4){
                if (nullbitmapvalue == 1){answer.add(null);}
                else{
                    long longBits = ((row[cur_pointer + 7] & 0xFFL) << 56) | ((row[cur_pointer + 6] & 0xFFL) << 48) | ((row[cur_pointer + 5] & 0xFFL) << 40) | ((row[cur_pointer + 4] & 0xFFL) << 32) | ((row[cur_pointer + 3] & 0xFFL) << 24) | ((row[cur_pointer + 2] & 0xFFL) << 16) | ((row[cur_pointer + 1] & 0xFFL) << 8) | (row[cur_pointer + 0] & 0xFFL);
                    double val = Double.longBitsToDouble(longBits);
                    answer.add(val);
                }
                cur_pointer += 8;
            }
            else{col_no += 1; break;}
            col_no += 1;

        }

        cur_pointer = 0;

        for (int i = 0 ; i < numberOfVariableLengthRecords(typeList) ; i++){

            int datatype = typeList.get(i+numberOfFixedLengthRecordsans);
            int nullbitmapvalue = (row[offsetnullbitmap + (col_no/8)]) & (1 << (7-(col_no%8)));

            // need to read the nullbitmap also to check if the value is null or the one read
            if (datatype == 0){
                if (nullbitmapvalue == 1){answer.add(null);}
                else{
                    int offsettoread = ((row[cur_pointer+1] << 8) | (row[cur_pointer + 0] & 0xFF));
                    int lengthtoread = ((row[cur_pointer+3] << 8) | (row[cur_pointer + 2] & 0xFF));      // little endian
                    byte[] stringarray = new byte[lengthtoread];
                    // for (int j = 0 ; j < lengthtoread ; j++){
                    for (int j = lengthtoread - 1 ; j >= 0 ; j--){                  // check if this is correct wrt big and little endian
                        stringarray[j] = row[offsettoread+j];
                    }
                    String string_to_add = new String(stringarray);
                    if (lengthtoread == 0){answer.add(null);}
                    else{answer.add(string_to_add);}
                    // System.out.println("printing offsettoread " + offsettoread + "priting legnth to read "  +lengthtoread);
                    // System.out.println("printing stringggg "+ stringarray);
                    // System.out.println("printing stringggg "+ new String(stringarray));
                }
            }
            col_no += 1;
            cur_pointer += 4;
        }
        return answer.toArray();
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid

        List<Object[]> answer = new ArrayList<>();

        if (!check_file_exists((table_name)) || block_id <= 0 || (get_data_block(table_name, block_id) == null)){return null;} // check if the block id is valid or not

        byte[] datablock = get_data_block(table_name, block_id);
        Block b = new Block(datablock);
        List<Integer> typeList = findDataTypes((table_name));

        int numRecords = ((datablock[0] << 8) | (datablock[1] & 0xFF));   // big endian

        int prev_offset = ((datablock[2] << 8) | (datablock[3] & 0xFF));
        int cur_offset = ((datablock[2] << 8) | (datablock[3] & 0xFF));    // big endian


        for (int i = 0 ; i < numRecords ; i++){
            cur_offset = ((datablock[(i+1)*2] << 8) | (datablock[(i+1)*2 + 1] & 0xFF));
            byte[] record;
            if (i == 0){
                record = b.get_data(cur_offset,b.get_block_capacity()-cur_offset);
            }
            else{
                record = b.get_data(cur_offset,prev_offset-cur_offset);
            }
            Object[] recordObject = convertToObjectArray((record), typeList);
            answer.add(recordObject);
            prev_offset = cur_offset;
        }

        return answer;
    }

    public boolean create_index(String table_name, String column_name, int order) {

        // finding the column number corresponding to the column name in database
        byte[] schemaBlock = get_data_block(table_name, 0);
        int numCols = ((schemaBlock[1] << 8) | (schemaBlock[0] & 0xFF));
        int cur_pointer = 2, col_no = 0, datatype = -1;
        String col_name ;
        boolean found = false;
        for (int i = 0; i < numCols; i++) {
            int offsetToRead = ((schemaBlock[cur_pointer+1] << 8) | (schemaBlock[cur_pointer ] & 0xFF));
            int lengthtoread = (int) schemaBlock[offsetToRead+1];
            byte[] stringarray = new byte[lengthtoread];
            for (int j = lengthtoread - 1 ; j >= 0 ; j--){                  // check if this is correct wrt big and little endian
                stringarray[j] = schemaBlock[offsetToRead+2+j];
            }
            col_name = new String(stringarray);
            if (col_name.equals(column_name)){
                col_no = i;
                found = true;
                datatype = (int) schemaBlock[offsetToRead];
                break;
            }
            cur_pointer += 2;
        }

        if (!found){return false;}
        // initialsing the b+tree
        int block_id = 1;
        if (datatype == 0){
            BPlusTreeIndexFile<String> b_plus_tree ;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);
            while (get_records_from_block(table_name, block_id) != null){
                List<Object[]> recordFile = get_records_from_block(table_name, block_id);
                int numRecords = recordFile.size();
                for (int i = 0 ; i < numRecords ; i++){
                    Object obj = recordFile.get(i)[col_no];
                    String key = (String) obj;
                    if (obj != null){
                        // System.out.println("going to insert");
                        b_plus_tree.insert(key, block_id);
                        // System.out.println("came out after insertion");
                    }
                }
                block_id += 1;
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(b_plus_tree);
            file_to_fileid.put(index_file_name, counter);
        }
        else if (datatype == 1){
            BPlusTreeIndexFile<Integer> b_plus_tree ;
            b_plus_tree = new BPlusTreeIndexFile<>(order, Integer.class);
            while (get_records_from_block(table_name, block_id) != null){
                List<Object[]> recordFile = get_records_from_block(table_name, block_id);
                int numRecords = recordFile.size();
                for (int i = 0 ; i < numRecords ; i++){
                    Object obj = recordFile.get(i)[col_no];
                    Integer key = (Integer) obj;
                    // System.out.println("idhar ");
                    // System.out.println("printing key " + key + " printing block id " + block_id);
                    if (obj != null){
                        b_plus_tree.insert(key, block_id);
                    }
                    // b_plus_tree.insert(key, block_id);
                    // System.out.println("but not here");
                }
                block_id += 1;
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(b_plus_tree);
            file_to_fileid.put(index_file_name, counter);
        }
        else if (datatype == 2){
            BPlusTreeIndexFile<Boolean> b_plus_tree ;
            b_plus_tree = new BPlusTreeIndexFile<>(order, Boolean.class);
            while (get_records_from_block(table_name, block_id) != null){
                List<Object[]> recordFile = get_records_from_block(table_name, block_id);
                int numRecords = recordFile.size();
                for (int i = 0 ; i < numRecords ; i++){
                    Object obj = recordFile.get(i)[col_no];
                    Boolean key = (Boolean) obj;
                    // b_plus_tree.insert(key, block_id);
                    if (obj != null){
                        b_plus_tree.insert(key, block_id);
                    }
                }
                block_id += 1;
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(b_plus_tree);
            file_to_fileid.put(index_file_name, counter);
        }
        else if (datatype == 3){
            BPlusTreeIndexFile<Float> b_plus_tree ;
            b_plus_tree = new BPlusTreeIndexFile<>(order, Float.class);
            while (get_records_from_block(table_name, block_id) != null){
                List<Object[]> recordFile = get_records_from_block(table_name, block_id);
                int numRecords = recordFile.size();
                for (int i = 0 ; i < numRecords ; i++){
                    Object obj = recordFile.get(i)[col_no];
                    Float key = (Float) obj;
                    // b_plus_tree.insert(key, block_id);
                    if (obj != null){
                        b_plus_tree.insert(key, block_id);
                    }
                }
                block_id += 1;
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(b_plus_tree);
            file_to_fileid.put(index_file_name, counter);
        }
        else if (datatype == 4){
            BPlusTreeIndexFile<Double> b_plus_tree ;
            b_plus_tree = new BPlusTreeIndexFile<>(order, Double.class);
            while (get_records_from_block(table_name, block_id) != null){
                List<Object[]> recordFile = get_records_from_block(table_name, block_id);
                int numRecords = recordFile.size();
                for (int i = 0 ; i < numRecords ; i++){
                    Object obj = recordFile.get(i)[col_no];
                    Double key = (Double) obj;
                    // b_plus_tree.insert(key, block_id);
                    if (obj != null){
                        b_plus_tree.insert(key, block_id);
                    }
                }
                block_id += 1;
            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(b_plus_tree);
            file_to_fileid.put(index_file_name, counter);
        }
        // System.out.println("rturneddd");

        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value, int datatype) {
        String index_file_name = table_name + "_" + column_name + "_index";
        int file_id = file_to_fileid.get(index_file_name);
        Object valueobj = null;
        if (datatype == 1) {
            valueobj = value.getValueAs(Integer.class);
        }
        else if (datatype == 2) {
            valueobj = value.getValueAs(Boolean.class);
        }
        else if (datatype == 3) {
            valueobj = value.getValueAs(Float.class);
        }
        else if (datatype == 4) {
            valueobj = value.getValueAs(Double.class);
        }
        else  {
            valueobj = value.getValueAs(String.class);
        }
        // System.out.println("going to find the vlaue " + valueobj);
        return db.search_index(file_id, valueobj);
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            // System.out.println("idhar hoon");
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}
