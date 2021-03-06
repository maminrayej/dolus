package manager.lock;

import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import java.util.Set;

/**
 * This class is responsible for periodically traversing the waiting graph and detecting cycles in it.
 *
 * @author m.amin rayej
 * @version 1.0
 * @since 1.0
 */
public class DeadLockDetectorRunnable implements Runnable {

    /**
     * Directed graph that represents the relationships between transactions and resources(lock tree elements)
     * basically is used for cycle detection -> dead lock
     */
    private final SimpleDirectedGraph<GraphNode, DefaultEdge> waitingGraph;

    private int period;


    public DeadLockDetectorRunnable(SimpleDirectedGraph<GraphNode, DefaultEdge> waitingGraph, int period) {
        this.waitingGraph = waitingGraph;

        this.period = period;
    }

    @Override
    public void run() {

        System.out.println("Dead lock detection is running");

        CycleDetector<GraphNode, DefaultEdge> cycleDetector = new CycleDetector<>(waitingGraph);

        while (true) {

            synchronized (waitingGraph) {
                Set<GraphNode> involveNodes = cycleDetector.findCycles();

                if (involveNodes.size() != 0) {
                    //TODO inform transaction manager of this cycle
                    System.out.println("Dead lock detected");
                }
            }

            try {
                Thread.sleep(period);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
