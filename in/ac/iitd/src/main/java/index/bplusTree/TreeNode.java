package index.bplusTree;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        // System.out.print(" | ");
        return;
    }

    default public byte[] convertintToBytes( int number){
        
        /* Write your code here */
        byte[] answer = new byte[2];
        answer[0] = (byte) ((number >> 8) & 0xFF);
        answer[1] = (byte) (number & 0xFF);
        return answer;
    }

    default public int convertBytesToint( byte[] byte_array){

        /* Write your code here */
        int ans;
        if (byte_array.length == 2){
            ans = ((byte_array[0] << 8) | (byte_array[1] & 0xFF));
        }
        else{
            ans = ((byte_array[0] & 0xFF) << 24) | ((byte_array[1] & 0xFF) << 16) | ((byte_array[2] & 0xFF) << 8) | (byte_array[3] & 0xFF);
        }
        return ans;
    }
    
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass) {
        if (typeClass == String.class) {
            return (T) new String(bytes); // Assume bytes represent UTF-8 encoded string
        } else if (typeClass == Integer.class) {
            // return (T) Integer.valueOf(convertBytesToInt(bytes));
            int value = convertBytesToint(bytes);
            return typeClass.cast(value);
        } else if (typeClass == Boolean.class) {
            return (T) Boolean.valueOf(bytes[0] != 0);
        } else if (typeClass == Float.class) {
            return (T) Float.valueOf(Float.intBitsToFloat(convertBytesToint(bytes)));
        } else if (typeClass == Double.class) {
            return (T) Double.valueOf(Double.longBitsToDouble(convertBytesToLong(bytes)));
        } else {
            // Handle unsupported type
            return null;
        }
    }

    default public byte[] convertTToBytes(T typeClassObject) {
        if (typeClassObject instanceof String) {
            return ((String) typeClassObject).getBytes(); // Assume UTF-8 encoding
        } else if (typeClassObject instanceof Integer) {
            return convertIntToBytes((Integer) typeClassObject);
        } else if (typeClassObject instanceof Boolean) {
            return new byte[] { (byte) (((Boolean) typeClassObject) ? 1 : 0) };
        } else if (typeClassObject instanceof Float) {
            return convertIntToBytes(Float.floatToIntBits((Float) typeClassObject));
        } else if (typeClassObject instanceof Double) {
            return convertLongToBytes(Double.doubleToLongBits((Double) typeClassObject));
        } else {
            return null;
        }
    }

    default public byte[] convertIntToBytes(int number) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((number >> 24) & 0xFF);
        bytes[1] = (byte) ((number >> 16) & 0xFF);
        bytes[2] = (byte) ((number >> 8) & 0xFF);
        bytes[3] = (byte) (number & 0xFF);
        return bytes;
    }


    default public byte[] convertLongToBytes(long number) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (number >> (56 - (i * 8)));
        }
        return bytes;
    }

    default public long convertBytesToLong(byte[] byteArray) {
        return ((long) (byteArray[0] & 0xFF) << 56) | ((long) (byteArray[1] & 0xFF) << 48) | ((long) (byteArray[2] & 0xFF) << 40)
                | ((long) (byteArray[3] & 0xFF) << 32) | ((long) (byteArray[4] & 0xFF) << 24) | ((long) (byteArray[5] & 0xFF) << 16)
                | ((long) (byteArray[6] & 0xFF) << 8) | (byteArray[7] & 0xFF);
    }
}