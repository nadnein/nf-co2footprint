package nextflow.co2footprint

import nextflow.NextflowMeta
import nextflow.Session
import nextflow.co2footprint.Records.CO2EquivalencesRecord
import nextflow.executor.NopeExecutor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskId
import nextflow.processor.TaskProcessor
import nextflow.processor.TaskRun
import nextflow.script.WorkflowMetadata
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

import java.time.OffsetDateTime
import java.util.concurrent.Executors
import groovy.util.logging.Slf4j


@Slf4j
class CO2FootprintObserverTest extends Specification{

    // ------ TEST UTILITY METHODS ------
    @Shared
    FileChecker fileChecker = new FileChecker()

    @Shared
    def traceRecord = new TraceRecord()

    def setupSpec() {
        traceRecord.putAll(
            [
                'task_id': '111',
                'process': 'observerTestProcess',
                'realtime': (1 as Long) * (3600000 as Long), // 1 h 
                 'cpus': 1,
                 'cpu_model': "Unknown model",
                 '%cpu': 100.0,
                 'memory': (7 as Long) * (1024**3 as Long), // 7 GB
                 'status': 'COMPLETED'
            ]
        )
    }

    private static BigDecimal round( double value ) {
        Math.round( value * 100 ) / 100
    }

    /**
     * Helper to create a mock session with a specific CI value.
     */
    private Session mockSessionWithCI(Path tracePath, Path summaryPath, Path reportPath, double ciValue) {
        return Mock(Session) {
            getConfig() >> [
                co2footprint: [
                    'traceFile': tracePath,
                    'summaryFile': summaryPath,
                    'reportFile': reportPath,
                    'ci': ciValue
                ]
            ]
        }
    }

    // ------ BASIC FUNCTIONALITY TESTS ------

    def 'should return observer' () {
        when:
        Session session = Mock(Session) { getConfig() >> [:] }
        List<TraceObserver> result = new CO2FootprintFactory().create(session)

        then:
        result.size() == 1
        result[0] instanceof CO2FootprintObserver
    }

    // ------ FULL RUN CALCULATION TESTS ------
    // The expected results were compared with the results from https://calculator.green-algorithms.org (v2.2), where the following values were used: 
    // - Running time: 1h
    // - Type of cores: CPU
    // - Number of cores: 1
    // - Model: Any
    // - Memory available: 7 GB
    // - Platform used: Personal computer
    // - Location: world 
    // - Usage factor: 1

    def 'test full run calculation of total CO2e and energy consumption with specific CI' () {
        given:
        Path tempPath = Files.createTempDirectory('tmpdir')
        Path tracePath = tempPath.resolve('trace_test.txt')
        Path summaryPath = tempPath.resolve('summary_test.txt')
        Path reportPath = tempPath.resolve('report_test.html')

        // Use helper to mock session with CI value 475.0
        Session session = mockSessionWithCI(tracePath, summaryPath, reportPath, 475.0)

        // Create task and handler
        TaskRun task = new TaskRun(id: TaskId.of(111))
        task.processor = Mock(TaskProcessor)
        TaskHandler handler = new NopeExecutor().createTaskHandler(task)

        // Create observer
        CO2FootprintFactory factory = new CO2FootprintFactory()
        CO2FootprintObserver observer = factory.create(session)[0] as CO2FootprintObserver

        observer.onFlowCreate(session)
        observer.onProcessStart(handler, traceRecord)
        observer.onProcessComplete(handler, traceRecord)

        expect:
        Double total_co2 = 0d
        Double total_energy = 0d
        observer.getCO2eRecords().values().each { co2Record ->
            total_energy += co2Record.getEnergyConsumption()
            total_co2 += co2Record.getCO2e()
        }
        // Energy consumption converted to Wh
        round(total_energy / 1000) == 13.61
        // Total CO2 in g (should reflect the CI value you set)
        round(total_co2 / 1000) == 6.46
    }

    def 'test full run with co2e equivalences calculation and specific CI' () {
        given:
        Path tempPath = Files.createTempDirectory('tmpdir')
        Path tracePath = tempPath.resolve('trace_test.txt')
        Path summaryPath = tempPath.resolve('summary_test.txt')
        Path reportPath = tempPath.resolve('report_test.html')

        // Use helper to mock session with CI value 475.0
        Session session = mockSessionWithCI(tracePath, summaryPath, reportPath, 475.0)

        // Create task and handler
        TaskRun task = new TaskRun(id: traceRecord.getTaskId())
        task.processor = Mock(TaskProcessor)
        TaskHandler handler = new NopeExecutor().createTaskHandler(task)

        // Create observer
        CO2FootprintFactory factory = new CO2FootprintFactory()
        CO2FootprintObserver observer = factory.create(session)[0] as CO2FootprintObserver

        observer.onFlowCreate(session)
        observer.onProcessStart(handler, traceRecord)
        observer.onProcessComplete(handler, traceRecord)

        // Accumulate CO2
        int total_co2 = 0d
        observer.getCO2eRecords().values().each { co2Record ->
            total_co2 += co2Record.getCO2e()
        }

        CO2EquivalencesRecord co2EquivalencesRecord = observer
            .getCO2FootprintComputer()
            .computeCO2footprintEquivalences(total_co2)

        expect:
        // Values compared to result from www.green-algorithms.org
        co2EquivalencesRecord.getCarKilometers().round(7) == 0.0369314 as Double
        co2EquivalencesRecord.getTreeMonths().round(7) == 0.007048 as Double
        co2EquivalencesRecord.getPlanePercent().round(7) == 0.012926 as Double
    }


    // ------ FILE CREATION TESTS ------

    def 'Should create correct trace, summary and report files' () {
        given:
        // Define temporary variables
        OffsetDateTime time = OffsetDateTime.now()
        Path tempPath = Files.createTempDirectory('tmpdir')
        Path tracePath = tempPath.resolve('trace_test.txt')
        Path summaryPath = tempPath.resolve('summary_test.txt')
        Path reportPath = tempPath.resolve('report_test.html')

        // Mock Session
        Session session = Mock(Session)
        session.getConfig() >> [
                co2footprint:
                        [
                                'traceFile': tracePath,
                                'summaryFile': summaryPath,
                                'reportFile': reportPath
                        ]
        ]
        session.getExecService() >> Executors.newFixedThreadPool(1)
        WorkflowMetadata meta = Mock(WorkflowMetadata)
        meta.scriptId >> 'MOCK'
        meta.start >> time
        meta.complete >> time
        meta.nextflow >> NextflowMeta.instance
        session.getWorkflowMetadata() >> meta

        // Create task
        TaskRun task = new TaskRun(id: TaskId.of(111))
        task.processor = Mock(TaskProcessor)
        TaskHandler taskHandler = new NopeExecutor().createTaskHandler(task)

        // Create Observer
        CO2FootprintObserver observer = new CO2FootprintFactory().create(session)[0] as CO2FootprintObserver

        when:
        // Run necessary observer steps
        observer.onFlowCreate(session)
        observer.onProcessComplete(taskHandler, traceRecord)
        observer.onFlowComplete()

        then:
        //
        // Check Trace File
        //
        fileChecker.checkIsFile(tracePath)
        List<String> traceLines = tracePath.readLines()
        traceLines.size() == 2

        traceLines[0].split('\t') as List<String> == [
                'task_id', 'status', 'name', 'energy_consumption', 'CO2e', 'time', 'carbon_intensity', 'cpus', 'powerdraw_cpu', 'cpu_model', 'cpu_usage', 'requested_memory'
        ]
        traceLines[1].split('\t') as List<String> == [
            '111', 'COMPLETED', 'null', '13.61 Wh', '6.53 g', 'null', '1ms', '480.0 gCO₂eq/kWh', 'null', '1', '11.0', 'Unknown model', '100.0', '7.0 B'
        ] // GA: CO2e is 6.94g with CI of 475 gCO2eq/kWh
        fileChecker.compareChecksums(tracePath, '12870c60ba2a982fd5b85ed0c3edf3ef')


        // Check Summary File
        fileChecker.runChecks(
                summaryPath,
                [
                        17: "reportFile: ${reportPath}",
                        18: "summaryFile: ${summaryPath}",
                        19: "traceFile: ${tracePath}"
                ]
        )

        // Check Report File
        fileChecker.runChecks(
            reportPath,
            [
            234: '          ' +
                    "<span id=\"workflow_start\">${time.format('dd-MMM-YYYY HH:mm:ss')}</span>" +
                    " - <span id=\"workflow_complete\">${time.format('dd-MMM-YYYY HH:mm:ss')}</span>",
            1303: '  window.options = [' +
                    '{"option":"ci","value":"480.0"},'+
                    '{"option":"customCpuTdpFile","value":null},' +
                    '{"option":"ignoreCpuModel","value":"false"},' +
                    '{"option":"location","value":null},' +
                    '{"option":"powerdrawCpuDefault","value":null},' +
                    '{"option":"powerdrawMem","value":"0.3725"},' +
                    '{"option":"pue","value":"1.0"},' +
                    "{\"option\":\"reportFile\",\"value\":\"${reportPath}\"}," +
                    "{\"option\":\"summaryFile\",\"value\":\"${summaryPath}\"}," +
                    "{\"option\":\"traceFile\",\"value\":\"${tracePath}\"}];"
            ]
        )
    }
}
