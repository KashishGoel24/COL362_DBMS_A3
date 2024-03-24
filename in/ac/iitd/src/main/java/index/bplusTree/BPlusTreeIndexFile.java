package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Stack;
import java.util.Arrays;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    private byte[] convertintToBytes( int number){

        /* Write your code here */
        byte[] answer = new byte[2];
        answer[0] = (byte) ((number >> 8) & 0xFF);
        answer[1] = (byte) (number & 0xFF);

        return answer;
    }

    private int convertBytesToint( byte[] byte_array){

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

    private T convertBytesToT(byte[] bytes, Class<T> typeClass) {
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

    private byte[] convertTToBytes(T typeClassObject) {
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
            // Handle unsupported type
            return null;
        }
    }

    private byte[] convertIntToBytes(int number) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (number >> 24);
        bytes[1] = (byte) (number >> 16);
        bytes[2] = (byte) (number >> 8);
        bytes[3] = (byte) number;
        return bytes;
    }

    private byte[] convertLongToBytes(long number) {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (number >> (56 - (i * 8)));
        }
        return bytes;
    }

    private long convertBytesToLong(byte[] byteArray) {
        return ((long) (byteArray[0] & 0xFF) << 56) | ((long) (byteArray[1] & 0xFF) << 48) | ((long) (byteArray[2] & 0xFF) << 40)
                | ((long) (byteArray[3] & 0xFF) << 32) | ((long) (byteArray[4] & 0xFF) << 24) | ((long) (byteArray[5] & 0xFF) << 16)
                | ((long) (byteArray[6] & 0xFF) << 8) | (byteArray[7] & 0xFF);
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

    // will be evaluated
    public void insert(T key, int block_id) {
        // add a root node case
        if (blocks.size() == 2){
            // System.out.println("went here 1");
            LeafNode<T> node ;
            node = (LeafNode<T>) blocks.get(getRootId());
            if (node.getNumKeys() < this.getOrder()-1){
                node.insert(key,block_id);
            }
            else{
                
                // LeafNode<T> newNode1 = new LeafNode<>(typeClass);  // see if the argument of leaf node here is fine
                // T[] keysRoot = node.getKeys();
                // T[] keysRoot2 = Arrays.copyOf(keysRoot, keysRoot.length + 1);
                // int insertIndex = keysRoot.length;
                // keysRoot2[insertIndex] = key;
                // Arrays.sort(keysRoot2);
                // int[] blockIdsRoot = node.getBlockIds();

                LeafNode<T> newNode1 = new LeafNode<>(typeClass);  // see if the argument of leaf node here is fine
                T[] keysRoot = node.getKeys();
                int[] blockIdsRoot = node.getBlockIds();

                T[] keysRoot2 = (T[]) new Object[keysRoot.length + 1];
                int[] blockIdsRoot2 = new int[keysRoot.length + 1];
                int m = 0, ins = 0;
                
                for (int l = 0 ; l < (keysRoot.length + 1) ; l++){
                    if ((m >= keysRoot.length) || (compare_greaterThan(keysRoot[m], key) && (ins == 0) && (m == 0 || compare_greaterThanEqualTo(key, keysRoot[m-1])))){
                        keysRoot2[l] = key;
                        blockIdsRoot2[l] = block_id;
                        ins = 1;
                    }
                    else{
                        keysRoot2[l] = keysRoot[m];
                        blockIdsRoot2[l] = blockIdsRoot[m];
                        m += 1;
                    }
                }
                
                // boolean done = false;
                // int jjj = (int) Math.floor((this.getOrder())/ 2.0);
                for (int i = (int) Math.floor((this.getOrder())/ 2.0) ; i < this.getOrder() ; i++){
                    // if ((jjj >= this.getOrder()-1)||(compare_equalto((keysRoot2[i]), key) && !done)){
                    //     newNode1.insert(keysRoot2[i],block_id);
                    //     done = true;}
                    // else{
                    //     newNode1.insert(keysRoot2[i],blockIdsRoot[jjj]);
                    //     jjj += 1;
                    // }
                    newNode1.insert(keysRoot2[i],blockIdsRoot2[i]);
                }
                // setting the new numkeys of the original leaf node to 0
                node.write_data(0,convertintToBytes((0)));
                // int kk = 0;
                for (int i = 0 ; i < (int) Math.floor((this.getOrder())/ 2.0) ; i++){
                    // if (compare_equalto((keysRoot2[i]), key) && !done){
                    //     node.insert(keysRoot2[i],block_id);
                    //     done = true;}
                    // else{
                    //     node.insert(keysRoot2[i],blockIdsRoot[kk]);
                    //     kk += 1;
                    // }
                    node.insert(keysRoot2[i],blockIdsRoot2[i]);
                }

                // setting the next node id of the original node
                node.write_data(4,convertintToBytes(blocks.size()));
                // setting the prev node id of the newly made node
                newNode1.write_data(2,convertintToBytes(1));
                
                // System.out.println("went here 2");
                // System.out.println("print the length do newndooe1.get keys " + newNode1.getKeys()[0]);
                InternalNode<T> newRootNode = new InternalNode<>(newNode1.getKeys()[0], this.getRootId(), blocks.size(), this.typeClass);
                blocks.add(newNode1);
                blocks.add(newRootNode);
                blocks.get(0).write_data(2,convertintToBytes(3));
            }

            return;

        }
        // System.out.println("went here 3");
        InternalNode<T> node1 ;
        int nextNode = getRootId();
        Stack<Integer> stack = new Stack<>();
        stack.push(this.getRootId());

        while (!isLeaf(nextNode)){
            node1 = (InternalNode<T>) blocks.get(nextNode);
            nextNode = node1.search(key);
            stack.push(nextNode);
        }

        // int prevLeafNodeId = convertBytesToint(blocks.get(nextNode).get_data(2,2));
        // while (prevLeafNodeId != -1){
            //     LeafNode<T> node_search = (LeafNode<T>) blocks.get(prevLeafNodeId);
            //     if (node_search.search(key) == 1){
                //         nextNode = prevLeafNodeId;
                //         prevLeafNodeId = convertBytesToint(blocks.get(nextNode).get_data(2,2));
                //     }
        //     else{break;}
        // }
        // stack.pop();
        // stack.push(nextNode);


        LeafNode<T> node ;
        node = (LeafNode<T>) blocks.get(nextNode);

        if (node.getNumKeys() < this.getOrder()-1){node.insert(key,block_id);}
        else{
            // System.out.println("went here 5");
            int currentleaf = stack.pop();   // popping the block id of the leaf node that waas stored

            LeafNode<T> newNode1 = new LeafNode<>(typeClass);  // see if the argument of leaf node here is fine
            T[] keysRoot = node.getKeys();
            int[] blockIdsRoot = node.getBlockIds();

            T[] keysRoot2 = (T[]) new Object[keysRoot.length + 1];
            int[] blockIdsRoot2 = new int[keysRoot.length + 1];
            int m = 0, inserted = 0;
            for (int l = 0 ; l < (keysRoot.length + 1) ; l++){
                if ((m >= keysRoot.length) || (compare_greaterThan(keysRoot[m], key) && (inserted == 0) && (m == 0 || compare_greaterThanEqualTo(key, keysRoot[m-1])))){
                    keysRoot2[l] = key;
                    blockIdsRoot2[l] = block_id;
                    inserted = 1;
                }
                else{
                    keysRoot2[l] = keysRoot[m];
                    blockIdsRoot2[l] = blockIdsRoot[m];
                    m += 1;
                }
            }
            // System.out.println("went here 6");

            for (int i = (int) Math.floor((this.getOrder())/ 2.0) ; i < this.getOrder() ; i++){
                newNode1.insert(keysRoot2[i],blockIdsRoot2[i]);
            }

            node.write_data(0,convertintToBytes(0));

            for (int i = 0 ; i < (int) Math.floor((this.getOrder())/ 2.0) ; i++){
                node.insert(keysRoot2[i],blockIdsRoot2[i]);
            }

            // setting the prev node id of the next node of the original non split node AND setting the next node id of the newl made node
            if (convertBytesToint(node.get_data(4, 2)) != -1){
                byte[] add1 = convertintToBytes(blocks.size());
                blocks.get(convertBytesToint(node.get_data(4, 2))).write_data(2, add1);
                byte[] add2 = node.get_data(4,2);
                newNode1.write_data(4,add2);
            }

            // setting the next node id of the original node
            byte[] nodeIdOfNextleafndoe = convertintToBytes(blocks.size());
            node.write_data(4,nodeIdOfNextleafndoe);

            // setting the prev node id of the newly made node
            byte[] nodeIdOfNextleafndoe2 = convertintToBytes(currentleaf);
            newNode1.write_data(2,nodeIdOfNextleafndoe2);

            int parent_node_id = stack.pop();
            int prev_parent_node_id = currentleaf;

            // int keytoWrite = convertBytesToint(convertTToBytes(newNode1.getKeys()[0]));
            T keytoWrite = newNode1.getKeys()[0];
            int blockIdtoWrite = blocks.size();
            blocks.add(newNode1);

            boolean flag = false;

            // System.out.println("went here 7");

            while (isFull(parent_node_id)){

                // System.out.println("went here 8");

                InternalNode<T>  parent_node1 = (InternalNode<T>) blocks.get(parent_node_id);
                InternalNode<T> internalNewNode1 = new InternalNode<T>(keytoWrite,0,blockIdtoWrite,this.typeClass);
                T[] keysinternal = parent_node1.getKeys();
                T[] keysInternal2 = (T[]) new Object[getOrder()];
                int[] blocksIdsInternal = parent_node1.getChildren();
                int[] blocksIdsInternal2 = new int[getOrder() + 1];
                blocksIdsInternal2[0] = blocksIdsInternal[0];
                int j = 0, inserted1 = 0;

                for (int i = 0 ; i < getOrder() ; i++){
                    if ((j > this.getOrder()-2) || ((blocksIdsInternal[j] == prev_parent_node_id) && (compare_greaterThanEqualTo(keysinternal[j], keytoWrite) && (inserted1 == 0)))){
                        keysInternal2[i] = keytoWrite;
                        blocksIdsInternal2[i+1] = blockIdtoWrite;
                        inserted1 = 1;
                    }
                    else{
                        keysInternal2[i] = keysinternal[j];
                        blocksIdsInternal2[i+1] = blocksIdsInternal[j+1];
                        j += 1;
                    }
                }

                int index_to_move_up = (int) (Math.floor((this.getOrder())/ 2.0));
                int first_index_for_second_parent = (int) (Math.floor((this.getOrder())/ 2.0)+1);
                internalNewNode1.write_data(4,convertintToBytes(blocksIdsInternal2[first_index_for_second_parent]));
                internalNewNode1.write_data(0,convertintToBytes(0));

                for (int i = first_index_for_second_parent ; i < this.getOrder() ; i++){
                    internalNewNode1.insert(keysInternal2[i],blocksIdsInternal2[i+1]);
                }

                parent_node1.write_data(0,convertintToBytes(0));

                for (int i = 0 ; i < index_to_move_up ; i++){
                    parent_node1.insert(keysInternal2[i],blocksIdsInternal2[i+1]);
                }

                keytoWrite = (keysInternal2[index_to_move_up]);
                blockIdtoWrite = blocks.size();
                blocks.add(internalNewNode1);

                if (stack.isEmpty()){
                    InternalNode<T> newrootnode = new InternalNode<>(keytoWrite,parent_node_id,blockIdtoWrite,this.typeClass);

                    blocks.get(0).write_data(2,convertintToBytes(blocks.size()));
                    blocks.add(newrootnode);
                    flag = true;
                    break;
                }
                else{
                    prev_parent_node_id = parent_node_id;
                    parent_node_id = stack.pop();
                }
            }
            // this is wrong
            if (!stack.isEmpty() || (!flag)){
                // System.out.println("went here 9");
                InternalNode<T>  parent_node11 = (InternalNode<T>) blocks.get(parent_node_id);
                parent_node11.insert(keytoWrite, blockIdtoWrite);
            }
        }

        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        // System.out.println("have come to searhc key: " + key);
        if (blocks.size() == 2){
            // System.out.println("in here 1");
            LeafNode<T> node ;
            node = (LeafNode<T>) blocks.get(getRootId());
            int ans = node.search(key);
            return ans;
        }
        // System.out.println("in here 2");
        InternalNode<T> node1 ;
        int nextNode = getRootId();
        while (!isLeaf(nextNode)){
            node1 = (InternalNode<T>) blocks.get(nextNode);
            nextNode = node1.search(key);
        }

        int prevLeafNodeId = convertBytesToint(blocks.get(nextNode).get_data(2,2));
        while (prevLeafNodeId != -1){
            LeafNode<T> node_search = (LeafNode<T>) blocks.get(prevLeafNodeId);
            if (node_search.search(key) == 1){
                nextNode = prevLeafNodeId;
                prevLeafNodeId = convertBytesToint(blocks.get(nextNode).get_data(2,2));
            }
            else{break;}
        }

        LeafNode<T> node ;
        node = (LeafNode<T>) blocks.get(nextNode);

        if (node.search(key) == 1){
            return nextNode;
        }
        else{
            boolean found = false;
            byte[] nextNode2bytes = node.get_data(4,2);
            int nextNode2 = (nextNode2bytes[0] << 8) | (nextNode2bytes[1] & 0xFF);
            while ((!found) && (nextNode2 != -1)){
                nextNode2bytes = node.get_data(4,2);
                int nextNode22 = (nextNode2bytes[0] << 8) | (nextNode2bytes[1] & 0xFF);
                node = (LeafNode<T>) blocks.get(nextNode22);
                if (node.search(key) == 1){
                    found = true;
                }
            }
            if (found){
                return nextNode2;
            }
            return -1;
        }
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}
