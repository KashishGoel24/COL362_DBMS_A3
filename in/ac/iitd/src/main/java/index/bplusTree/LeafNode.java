package index.bplusTree;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    // initialisation of the leaf node of the b+ tree
    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        // set the prev node id value to -1
        this.write_data(2, convertintToBytes(-1));

        // set next node id value to -1
        this.write_data(4, convertintToBytes(-1));

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int cur_start_index = 10;
        for (int i = 0; i < numKeys ; i++){
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            byte[] keys_object = this.get_data(cur_start_index+2,len_key);
            keys[i] = convertBytesToT(keys_object,this.typeClass);
            cur_start_index += (4 + len_key);
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();
        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int cur_start_index = 10;
        for (int i = 0; i < numKeys ; i++){
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            byte[] block_ids_data = this.get_data(cur_start_index-2,2);
            block_ids[i] = (block_ids_data[0] << 8) | (block_ids_data[1] & 0xFF);
            cur_start_index += (4 + len_key);
        }

        return block_ids;
    }

    private boolean compare_greaterThanEqualTo(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return (Integer) k1 >= (Integer) k2;
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) >= ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return (Float) k1 >= (Float) k2;
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) >= 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return (Double) k1 >= (Double) k2;
        } else {
            return false;
        }
    }

    private boolean compare_greaterThan(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return (Integer) k1 > (Integer) k2;
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) > ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return (Float) k1 > (Float) k2;
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) >= 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return (Double) k1 > (Double) k2;
        } else {
            return false;
        }
    }

    @Override
    public void insert(T key, int block_id) {

        byte[] ourKey1 = convertTToBytes(key);
        byte[] blockIdBytes = convertintToBytes(block_id);
        int lenkeyour = ourKey1.length;
        byte[] ourlenKey = convertintToBytes(lenkeyour);

        T[] keys = this.getKeys();
        int numKeys = getNumKeys();
        int[] blockIdsArray = this.getBlockIds();
        int cur_start_index = 10;
        int start = 0;
        for (int i = 0 ; i < numKeys ; i++){
            // if (compare_greaterThanEqualTo((keys[i]), key) && ((i == 0) || compare_greaterThan(key,keys[i-1]))){
            //     start = i;
            //     break;
            // }
            if ((compare_greaterThan(keys[i], key)) && ((i == 0) || compare_greaterThanEqualTo(key,keys[i-1]))){
                start = i; break;
            }
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            cur_start_index += (4 + len_key);
            if (i == numKeys - 1){start = i+1;}
        }

        this.write_data(cur_start_index-2,blockIdBytes);
        this.write_data(cur_start_index,ourlenKey);
        this.write_data(cur_start_index+2,ourKey1);
        cur_start_index += (4+lenkeyour);
        while (start < numKeys){
            byte[] keyByteElement = convertTToBytes(keys[start]);
            byte[] blockIdElement = convertintToBytes(blockIdsArray[start]);
            byte[] lenKeyElement = convertintToBytes(keyByteElement.length);
            this.write_data(cur_start_index-2,blockIdElement);
            this.write_data(cur_start_index,lenKeyElement);
            this.write_data(cur_start_index+2,keyByteElement);
            cur_start_index += (4 + keyByteElement.length);
            start += 1;
        }

        byte[] newNumKeys = convertintToBytes(1+numKeys) ;
        this.write_data(0,newNumKeys);

        cur_start_index -= 2;
        byte[] newNextFreeOffset = convertintToBytes(cur_start_index);
        this.write_data(6,newNextFreeOffset);
        return;

    }

    // can be used as helper function - won't be evaluated
    // returning -1 if the key value doesnt exist in the b+ tree else returning 1
    @Override
    public int search(T key) {

        T[] keys = this.getKeys();
        int numKeys = getNumKeys();
        for (int i = 0; i < numKeys ; i++){
            if (compareValues(key, keys[i]) == 0){
                return 1;
            }
            else if (compare_greaterThan((keys[i]), key) && (i == 0 || compare_greaterThan(key, keys[i-1]))){
                return 1;
            }
        }

        return -1;
    }

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

}
