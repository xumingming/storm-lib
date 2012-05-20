package storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

/**
 * This demo scheduler make sure a spout named <code>special-spout</code> in topology <code>special-topology</code> runs
 * on a supervisor named <code>special-supervisor</code>. supervisor does not have name? You can configure it through
 * the config: <code>supervisor.scheduler.meta</code> you can configure anything in it.
 * 
 * @author xumingmingv May 19, 2012 11:10:43 AM
 */
public class DemoScheduler implements IScheduler {

    public void schedule(Topologies topologies, Cluster cluster) {
        // Gets the topology which we want to schedule
        TopologyDetails topology = topologies.getByName("special-topology");

        // make sure the special topology is submitted,
        if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            System.out.println("Our special topology needs scheduling.");
            if (needsScheduling) {
                // find out all the needs-scheduling components of this topology
                Map<String, List<Integer>> componentToTasks = cluster.getNeedsSchedulingComponentToTasks(topology);
                if (componentToTasks.containsKey("special-spout")) {
                    System.out.println("Found the special-spout.");
                    List<Integer> tasks = componentToTasks.get("special-spout");

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
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        // if there is no available slots on this supervisor, free some.
                        if (availableSlots.isEmpty() && !tasks.isEmpty()) {
                            for (Integer task : specialSupervisor.getAllPorts()) {
                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), task));
                            }
                        }

                        // re-get the aviableSlots
                        availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        int taskPerSlot = tasks.size() / availableSlots.size();

                        // calculate how many tasks per slot
                        if (taskPerSlot * availableSlots.size() < tasks.size()) {
                            taskPerSlot += 1;
                        }

                        // since it is just a demo, to keep things simple, we assign all the
                        // tasks into one slot.
                        cluster.assign(availableSlots.get(0), topology.getId(), tasks);
                        System.out.println("We assigned tasks:" + tasks + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
                    }
                }
            }
        }
        new backtype.storm.scheduler.EvenScheduler().schedule(topologies, cluster);
    }

}
