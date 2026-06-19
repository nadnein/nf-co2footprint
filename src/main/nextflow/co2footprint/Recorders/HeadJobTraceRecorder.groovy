package nextflow.co2footprint.Recorders

import com.sun.management.OperatingSystemMXBean
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.trace.TraceRecord
import oshi.SystemInfo
import oshi.hardware.CentralProcessor
import oshi.software.common.os.linux.LinuxOperatingSystem
import oshi.software.os.OSProcess
import oshi.software.os.OperatingSystem
import oshi.util.driver.linux.proc.ProcessStat
import oshi.util.tuples.Triplet

import java.lang.management.ManagementFactory
import java.lang.management.RuntimeMXBean

/**
 * A Recorder of trace values for a Nextflow head job, which can be attached after startup
 * to capture timepoints before workflow invocation.
 */
@Slf4j
class HeadJobTraceRecorder {
    // Constants
    static final String headJobSuffix = 'head_job'
    
    // OSHI info handles
    private final RuntimeMXBean          runtimeBean = ManagementFactory.getRuntimeMXBean()
    private final OperatingSystemMXBean  osBean      = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private final SystemInfo             systemInfo  = new SystemInfo()
    private final CentralProcessor       processor   = systemInfo.hardware.processor
    private final OperatingSystem        os          = systemInfo.operatingSystem

    // Sampling settings
    private final Timer timer = new Timer('head-job-trace-recorder', true)

    // Process information
    private int pid
    private OSProcess rootProcess
    private Map<OSProcess, Set<OSProcess>> headProcesses = [:].asSynchronized()

    // Aggregation
    final List<MemorySample> samples = [].asSynchronized() as List<MemorySample>
    final TraceRecord headJobRecord = new TraceRecord()

    /**
     * Start the recording of a head job.
     */
    void start() {
        pid = runtimeBean.pid as int
        rootProcess = os.getProcess(pid)
        headProcesses.put(rootProcess, [] as Set)

        headJobRecord.putAll(
                [
                        task_id:        '-1',
                        container:      'JVM',
                        tag:            'Head job',
                        status:         'SUBMITTED',
                        submit:         runtimeBean.startTime,
                        attempt:        0,
                        cpus:           osBean.availableProcessors,
                        cpu_model:      processor.processorIdentifier.name
                ]
        )
    }

    /**
     * Attach a Nextflow {@link Session} to the Recorder.
     *
     * @param session The current Nextflow session
     */
    void attachSession(Session session) {
        // Start sampling for memory
        timer.scheduleAtFixedRate(new TimerTask() { void run() { sample() } } , 0, 500)

        headJobRecord.putAll(
                [
                        hash:           session.hashCode(),
                        native_id:      pid as String,
                        process:        'head job',
                        name:           session.getRunName() + '-' + headJobSuffix,
                        status:         'STARTED',
                        start:          System.currentTimeMillis(),
                        attempt:        headJobRecord.store.get('attempt', 0) + 1
                ]
        )
    }

    /**
     * Create a finalized head job specific {@link TraceRecord} from the current samples.
     */
    TraceRecord report() {
        long endTimestamp = System.currentTimeMillis()

        // Reduce the process to values
        Set<OSProcess> processList = headProcesses.keySet()
        processList.each({ OSProcess p -> p.updateAttributes() })

        Double cpuUsage = getCPUUsage(headProcesses)
        System.out.println("getProcessCpuLoadCumulative: ${cpuUsage}")
        System.out.println("Included children (Linux): ${getCPUUsage(headProcesses, true, true)}")
        System.out.println("Included children: ${getCPUUsage(headProcesses, true, false)}")
        System.out.println("Without children (Linux): ${getCPUUsage(headProcesses, false, true)}")
        System.out.println("Without children: ${getCPUUsage(headProcesses, false, false)}")

        headJobRecord.putAll(
                [
                        status:         'COMPLETED',
                        complete:       endTimestamp,
                        duration:       endTimestamp - (headJobRecord.get('submit') as long),
                        realtime:       runtimeBean.uptime,
                        memory:         Runtime.getRuntime().maxMemory(),
                        '%cpu':         cpuUsage * 100,
                        read_bytes:     processList.collect({ OSProcess p -> p.bytesRead}).sum() as Long,
                        write_bytes:    processList.collect({ OSProcess p -> p.bytesWritten}).sum() as Long,
                        vol_ctxt:       processList.collect({ OSProcess p -> p.minorFaults}).sum() as Long,
                        inv_ctxt:       processList.collect({ OSProcess p -> p.majorFaults}).sum() as Long,
                ] 
        )

        if (samples) {
            List<Long> rss = samples.collect({ MemorySample sample -> sample.rssBytes})
            List<Long> vmem = samples.collect({ MemorySample sample -> sample.virtualMemoryBytes})
            headJobRecord.putAll(
                    [
                            memory:         rss.average(),
                            rss:            rss.average(),
                            vmem:           vmem.average(),
                            peak_rss:       rss.max(),
                            peak_vmem:      vmem.max(),
                    ]
            )
        }

        return headJobRecord
    }

    /**
     * Stop the sampling and finish accumulating the information in the TraceRecord.
     */
    void stop() {
        timer.cancel()
        timer.purge()
    }

    /**
     * Determine CPU usage of a list of processes. Either with their children (only works on Linux) or only of themselves.
     * 
     * @param processes List of processes
     * @param includeChildWait Whether or not to include the wait time for children
     * @return The number of computing cores that were used on average by the process over its execution time
     */
    Double getCPUUsage(Map<OSProcess, Set<OSProcess>> processes, boolean includeChildWait=false, boolean useLinux=true) {
        // Linux can just exclude wait time to get the isolated process CPU load
        if (useLinux && (os instanceof LinuxOperatingSystem)) {
            Long tickFrequency = (os as LinuxOperatingSystem).getHz()
            return processes.keySet().collect( { OSProcess process ->
                // Accumulate the running ticks of the process (including waiting time for children)
                Long processTime = process.userTime + process.kernelTime

                if (includeChildWait) {
                    Map<ProcessStat.PidStat, Long> pidStats = getPidStats(process.processID)
                    Long childrenTime = pidStats.get(ProcessStat.PidStat.CUTIME) + pidStats.get(ProcessStat.PidStat.CSTIME)
                    
                    // Convert from jiffies to ms
                    processTime +=  (childrenTime * 1000 / tickFrequency) as Long
                }

                 processTime / process.upTime
            }).sum() as double
        }
        // Other OS need to subtract the child load from the parent
        else {
            processes.collect({ OSProcess process, Set<OSProcess> children ->
                Long processTime = process.userTime + process.kernelTime
                if (includeChildWait) {
                    children.each({ OSProcess child ->
                        child.updateAttributes()
                        processTime += child.userTime + child.kernelTime
                    })
                }
                processTime / process.upTime
            }).sum() as double
        }
    }

    /**
     * Attempts to fetch the PID stats for the current process.
     *
     * @param pid The PID for which to fetch the stats
     * @return Map of PID stats, or null if the information could not be retrieved (e.g. due to permissions or OS)
     */
    Map<ProcessStat.PidStat, Long> getPidStats(Integer pid) {
        LinuxOperatingSystem
        if (os instanceof LinuxOperatingSystem) {
            try {
                Triplet<String, Character, Map<ProcessStat.PidStat, Long>> stats = ProcessStat.getPidStats(pid)
                return stats.getC()
            }
            catch (Exception e) {
                log.trace("Failed to get process stats for PID ${pid}: ${e.message}")
            }
        }

        return null
    }

    /**
     * A predicate to exclude Nextflow Task Runs
     */
    Closure<OSProcess> noNextflowTaskRuns = { OSProcess process -> !process.commandLine.contains(TaskRun.CMD_RUN) }

    /**
     * Collect all descendants that are part of the head job recursively by excluding Nextflow Task Runs
     * 
     * @param process The root process for which the head job descendents are searched
     * @return All descendent processes that are not associated with tasks
     */
    List<OSProcess> collectHeadDescendants(OSProcess process) {
        List<OSProcess> allChildren = os.getChildProcesses(process.processID, null, null, 0)
        
        // Collect information on process child relationship
        if (headProcesses.containsKey(process)) {
            headProcesses[process].addAll( allChildren )
        }
        else {
            headProcesses[process] = allChildren as Set<OSProcess>
        }
        
        List<OSProcess> headChildren = allChildren.findAll( noNextflowTaskRuns )
        List<OSProcess> headDescendents = [process]
        
        headChildren.each { OSProcess childProcess ->
            headDescendents.addAll( collectHeadDescendants(childProcess) )
        }
        
        return headDescendents
    }

    /**
     * Sample memory information and update children.
     */
    void sample(OSProcess process=rootProcess) {
        // Update root process information
        process.updateAttributes()
        
        // Collect active descendant head job processes
        List<OSProcess> activeProcesses = collectHeadDescendants(process)

        if (process != null) {
            MemorySample sample = new MemorySample(
                    timestamp: System.currentTimeMillis(),
                    
                    // Memory - The RSS value gives the best approximation for allocated resources by the head job
                    rssBytes: activeProcesses.collect({ OSProcess p -> p.residentMemory}).sum() as Long,
                    virtualMemoryBytes: activeProcesses.collect({ OSProcess p -> p.virtualSize}).sum() as Long,
            )

            samples.add(sample)
        }
    }
}
