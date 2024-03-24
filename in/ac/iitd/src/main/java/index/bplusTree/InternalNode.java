package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        // System.out.println("printntttttt " + convertBytesToint(this.get_data(4,2)));

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
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

    private boolean compare_equalto(T k1, T k2){
        if (k1 instanceof Integer && k2 instanceof Integer) {
            return (Integer) k1 == (Integer) k2;
        } else if (k1 instanceof Boolean && k2 instanceof Boolean) {
            return ((Boolean) k1 ? 1 : 0) == ((Boolean) k2 ? 1 : 0);
        } else if (k1 instanceof Float && k2 instanceof Float) {
            return (Float) k1 == (Float) k2;
        } else if (k1 instanceof String && k2 instanceof String) {
            return ((String) k1).compareTo((String) k2) == 0;
        } else if (k1 instanceof Double && k2 instanceof Double) {
            return (Double) k1 == (Double) k2;
        } else {
            return false;
        }
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int cur_start_index = 6;
        for (int i = 0; i < numKeys ; i++){
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            byte[] keys_object = this.get_data(cur_start_index+2,len_key);
            keys[i] = convertBytesToT(keys_object,this.typeClass);          // change needed -> correct this
            cur_start_index += (4 + len_key);
        }

        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {

        // System.out.println(" dsjvdvdjkvdv");
        // if (convertBytesToint((convertTToBytes(key))) == -1 && right_block_id == -1){return;}
        byte[] ourKey1 = convertTToBytes(key);
        byte[] blockIdBytes = convertintToBytes(right_block_id);
        int lenkeyour = ourKey1.length;
        byte[] ourlenKey = convertintToBytes(lenkeyour);

        T[] keys = this.getKeys();
        int numKeys = getNumKeys();
        int[] childrenArray = this.getChildren();
        int cur_start_index = 6;
        int start = 0;
        for (int i = 0 ; i < numKeys ; i++){
            if ((compare_greaterThan(keys[i], key)) && ((i == 0) || compare_greaterThanEqualTo(key,keys[i-1]))){
                start = i; break;
            }
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            cur_start_index += (4 + len_key);
            if (i == numKeys - 1){start = i+1;}
        }
        this.write_data(cur_start_index+2+lenkeyour,blockIdBytes);
        this.write_data(cur_start_index,ourlenKey);
        this.write_data(cur_start_index+2,ourKey1);
        cur_start_index += (4+lenkeyour);
        while (start < numKeys){
            byte[] keyByteElement = convertTToBytes(keys[start]);
            byte[] blockIdElement = convertintToBytes(childrenArray[start+1]);
            byte[] lenKeyElement = convertintToBytes(keyByteElement.length);
            this.write_data(cur_start_index+2+convertBytesToint(lenKeyElement),blockIdElement);
            this.write_data(cur_start_index,lenKeyElement);
            this.write_data(cur_start_index+2,keyByteElement);
            cur_start_index += (4 + keyByteElement.length);
            start += 1;
        }

        byte[] newNumKeys = convertintToBytes(numKeys+1);
        this.write_data(0,newNumKeys);


        // cur_start_index -= 2;        // see this that next free offset will be from the length index itself
        byte[] newNextFreeOffset = convertintToBytes((cur_start_index)) ; // make this usig cur start index
        this.write_data(2,newNextFreeOffset);

        return;

    }


    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);
        int[] children = new int[numKeys + 1];

        int cur_start_index = 6;
        for (int i = 0; i < numKeys+1 ; i++){
            byte[] lenKeyBytes = this.get_data(cur_start_index,2);
            int len_key = (lenKeyBytes[0] << 8) | (lenKeyBytes[1] & 0xFF);
            byte[] children_data = this.get_data(cur_start_index-2,2);
            children[i] = (children_data[0] << 8) | (children_data[1] & 0xFF);
            cur_start_index += (4 + len_key);
        }
        return children;

    }

    // can be used as helper function - won't be evaluated

    // in this function retuning the index corresp to the pointer of who in children
    // we can get the next node to find
    @Override
    public int search(T key) {
        /* Write your code here */
        int numKeys = getNumKeys();
        T[] keys = this.getKeys();
        int[] children = this.getChildren();

        for (int i = 0 ; i < numKeys ; i++){
            if (compareValues(key, keys[i]) == 0 && ((i == numKeys - 1) || !(compareValues(key, keys[i+1]) == 0))){
                return children[i+1];
                // return children[i];
            }
            else if (compare_greaterThan((keys[i]), key) && (i == 0 || compare_greaterThan(key, keys[i-1]))){
                return children[i];
            }
        }

        return children[numKeys];

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
