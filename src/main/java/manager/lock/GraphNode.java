package manager.lock;

/**
 * This class represents a node in waiting graph
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class GraphNode {

    /**
     * Different types of graph node
     * 1- transaction node: this node points to an active transaction
     * 2- resource node: this node points to  an element in lock tree
     */
    public static final int TRANSACTION_NODE = 1;
    public static final int RESOURCE_NODE = 2;

    /**
     * Element that this node contains
     */
    private Object element;

    /**
     * type of the element
     */
    private int nodeType;

    /**
     * Default constructor
     *
     * @param element element to be contained in this node
     * @param nodeType type of the element
     * @since 1.0
     */
    public GraphNode(Object element, int nodeType) {
        this.element = element;
        this.nodeType = nodeType;
    }

    /**
     * Get element contained in this graph node
     * @return element in this node
     * @since 1.0
     */
    public Object getElement() {
        return element;
    }

    /**
     * Get type of the element contained in this node
     *
     * @return type of the element
     * @since 1.0
     */
    public int getNodeType() {
        return nodeType;
    }

}
