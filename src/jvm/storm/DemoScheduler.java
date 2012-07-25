package storm;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
 * on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
 * the config: <code>supervisor.scheduler.meta</code> -- actually you can put any config you like in this config item.
 * 
 * In our example, we need to put the following config in supervisor's <code>storm.yaml</code>:
 * <pre>
 *     # give our supervisor a name: "special-supervisor"
 *     supervisor.scheduler.meta:
 *       name: "special-supervisor"
 * </pre>
 * 
 * Put the following config in <code>nimbus</code>'s <code>storm.yaml</code>:
 * <pre>
 *     # tell nimbus to use this custom scheduler
 *     storm.scheduler: "storm.DemoScheduler"
 * </pre>
 * @author xumingmingv May 19, 2012 11:10:43 AM
 */
public class DemoScheduler implements IScheduler {

    public void schedule(Topologies topologies, Cluster cluster) {
    	System.out.println("DemoScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("special-topology");

        // make sure the special topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
            	System.out.println("Our special topology DOES NOT NEED scheduling.");
            } else {
            	System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName("special-topology").getId());
                if (currentAssignment != null) {
                	System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                	System.out.println("current assignments: {}");
                }
                
                if (!componentToExecutors.containsKey("special-spout")) {
                	System.out.println("Our special-spout DOES NOT NEED scheduling.");
                } else {
                    System.out.println("Our special-spout needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("special-spout");

                    // find out the our "special-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails specialSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("special-supervisor")) {
                            specialSupervisor = supervisor;
                            break;
                        }
                    }

                    // found the special supervisor
                    if (specialSupervisor != null) {
                    	System.out.println("Found the special-supervisor");
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        
                        // if there is no available slots on this supervisor, free some.
                        // TODO for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                            }
                        }

                        // re-get the aviableSlots
                        availableSlots = cluster.getAvailableSlots(specialSupervisor);

                        // since it is just a demo, to keep things simple, we assign all the
                        // executors into one slot.
                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
                        System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
                    } else {
                    	System.out.println("There is no supervisor named special-supervisor!!!");
                    }
                }
            }
        }
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

}
