package nextflow.co2footprint.Recorders

import com.sun.management.OperatingSystemMXBean
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.trace.TraceRecord
import oshi.SystemInfo
import oshi.driver.linux.proc.ProcessStat
import oshi.hardware.CentralProcessor
import oshi.hardware.GlobalMemory
import oshi.software.os.OSProcess
import oshi.software.os.OperatingSystem
import oshi.software.os.linux.LinuxOperatingSystem
import oshi.util.tuples.Triplet

import java.lang.management.ManagementFactory
import java.lang.management.RuntimeMXBean
import java.util.function.Predicate

/**
 * A Recorder of trace values for a Nextflow session, which can be attached after startup
 * to capture timepoints before workflow invocation.
 */
@Slf4j
class SessionTraceRecorder {
    // OSHI info handles
    private final RuntimeMXBean          runtimeBean = ManagementFactory.getRuntimeMXBean()
    private final OperatingSystemMXBean  osBean      = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private final SystemInfo             systemInfo  = new SystemInfo()
    private final CentralProcessor       processor   = systemInfo.hardware.processor
    private final GlobalMemory           memory      = systemInfo.hardware.memory
    private final OperatingSystem        os          = systemInfo.operatingSystem

    // Sampling settings
    private final Timer timer = new Timer('session-trace-recorder', true)

    // Process information
    private int pid
    private OSProcess process
    private Map<Integer, OSProcess> allProcesses = [:].asSynchronized()

    // Aggregation
    final List<MemorySample> samples = [].asSynchronized() as List<MemorySample>
    final TraceRecord sessionRecord = new TraceRecord()

    /**
     * Start the recording of a session.
     */
    void start() {
        pid = runtimeBean.pid as int
        process = os.getProcess(pid)
        allProcesses.put(pid, process)

        sessionRecord.putAll(
                [
                        container:      'JVM',
                        tag:            'Session',
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

        sessionRecord.putAll(
                [
                        task_id:        '-1',
                        hash:           session.hashCode(),
                        native_id:      pid as String,
                        process:        'session',
                        name:           session.getRunName() + '-session',
                        status:         'STARTED',
                        start:          System.currentTimeMillis(),
                        attempt:        sessionRecord.store.get('attempt', 0) + 1
                ]
        )
    }

    /**
     * Create a finalized session specific {@link TraceRecord} from the current samples.
     */
    TraceRecord report() {
        long endTimestamp = System.currentTimeMillis()

        // Reduce the process to values
        List<OSProcess> processList = allProcesses.values() as List<OSProcess>
        processList.each({ OSProcess p -> p.updateAttributes() })

        Map<ProcessStat.PidStat, Long> rootStats = getPidStats(pid)

        // Determine CPU usage
        Double cpuUsage
        if (rootStats != null) {
            // Accumulate the running ticks of root including waiting time for children
            Long cpuTime = rootStats.get(ProcessStat.PidStat.UTIME) + rootStats.get(ProcessStat.PidStat.STIME) +
                    rootStats.get(ProcessStat.PidStat.CUTIME) + rootStats.get(ProcessStat.PidStat.CSTIME)
            // Convert from jiffies to ms
            cpuTime = (cpuTime * 1000 / LinuxOperatingSystem.getHz()) as Long

            // Calculate elapsed time since process start
            Long elapsedTime = endTimestamp - process.startTime

            cpuUsage = cpuTime / elapsedTime
            log.debug("Calculated CPU usage for PID ${pid}: ${cpuUsage}")
        }
        else {
            cpuUsage = processList.collect({ OSProcess p -> p.getProcessCpuLoadCumulative() }).sum() as double
        }

        sessionRecord.putAll(
                [
                        status:         'COMPLETED',
                        complete:       endTimestamp,
                        duration:       endTimestamp - (sessionRecord.get('submit') as long),
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
            sessionRecord.putAll(
                    [
                            rss:            rss.average(),
                            vmem:           vmem.average(),
                            peak_rss:       rss.max(),
                            peak_vmem:      vmem.max(),
                    ]
            )
        }

        return sessionRecord
    }

    /**
     * Stop the sampling and finish accumulating the information in the TraceRecord.
     */
    void stop() {
        timer.cancel()
        timer.purge()
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
    Predicate<OSProcess> noNextflowTaskRuns = { OSProcess process -> process.commandLine.contains(TaskRun.CMD_RUN) }

    /**
     * Collect all descendants that are part of the head job recursively by excluding Nextflow Task Runs
     * 
     * @param process The root process for which the head job descendents are searched
     * @return All descendent processes that are not associated with tasks
     */
    List<OSProcess> collectHeadDescendants(OSProcess process) {
        allProcesses[process.processID] = process
        
        List<OSProcess> headChildren = os.getChildProcesses(process.processID, noNextflowTaskRuns, null, 0)
        
        headChildren.each { OSProcess childProcess -> headChildren.addAll( collectHeadDescendants(childProcess) ) }
        headChildren.add(process)
        
        return headChildren
    }

    /**
     * Sample memory information and update children.
     */
    void sample() {
        // Update root process information
        process.updateAttributes()
        
        // Collect active descendant processes
        List<OSProcess> activeProcesses = collectHeadDescendants(process)

        if (process != null) {
            MemorySample sample = new MemorySample(
                    timestamp: System.currentTimeMillis(),

                    // Memory
                    rssBytes: activeProcesses.collect({ OSProcess p -> p.residentSetSize}).sum() as Long,
                    virtualMemoryBytes: activeProcesses.collect({ OSProcess p -> p.virtualSize}).sum() as Long,
            )

            samples.add(sample)
        }
    }
}
