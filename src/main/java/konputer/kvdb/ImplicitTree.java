package konputer.kvdb;

import java.util.ArrayList;

public class ImplicitTree<T> {

    ArrayList<T> items;

    public ImplicitTree() {
        items = new ArrayList<>();
    }

    public void add(T item) {
        items.add(item);
    }

    public T at(int index) {
        return items.get(index);
    }

    public int parentOf(int index) {
        if (index <= 0 || index >= items.size()) {
            return -1; // No parent for root or invalid index
        }
        return (index - 1) / 2;
    }

    public int leftChildOf(int index) {
        int leftIndex = 2 * index + 1;
        return (leftIndex < items.size()) ? leftIndex : -1; // Return -1 if no left child
    }

    public int rightChildOf(int index) {
        int rightIndex = 2 * index + 2;
        return (rightIndex < items.size()) ? rightIndex : -1; // Return -1 if no right child
    }

}
