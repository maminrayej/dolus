package manager.lock;

import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.generate.GnmRandomBipartiteGraphGenerator;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

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

        CycleDetector<GraphNode, DefaultEdge> cycleDetector = new CycleDetector<>(waitingGraph);

        while (true) {

            synchronized (waitingGraph) {
                Set<GraphNode> involveNodes = cycleDetector.findCycles();

                if (involveNodes.size() != 0) {
                    //TODO inform transaction manager of this cycle
                }
            }

            try {
                Thread.sleep(period);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
