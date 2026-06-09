package kr.co.vincent.rag.service;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@Service
public class MonitorService {

	private final MeterRegistry meterRegistry;

	public MonitorService( MeterRegistry meterRegistry ) {
		this.meterRegistry = meterRegistry;
	}

	public Map<String, Object> getServerMetrics() {
		Map<String, Object> metrics = new HashMap<>();

		double systemCpu = meterRegistry.find( "system.cpu.usage" ).gauge() != null ?
			meterRegistry.find( "system.cpu.usage" ).gauge().value() * 100 : 0.0;
		double processCpu = meterRegistry.find( "process.cpu.usage" ).gauge() != null ?
			meterRegistry.find( "process.cpu.usage" ).gauge().value() * 100 : 0.0;

		metrics.put( "systemCpu", String.format( "%.2f", systemCpu ) );
		metrics.put( "processCpu", String.format( "%.2f", processCpu ) );

		OperatingSystemMXBean osBean = ( OperatingSystemMXBean ) ManagementFactory.getOperatingSystemMXBean();

		long totalPhysicalMemory = 0;
		long freePhysicalMemory = 0;

		try {
			Method totalMethod = osBean.getClass().getMethod( "getTotalMemorySize" );
			Method freeMethod = osBean.getClass().getMethod( "getFreeMemorySize" );

			totalPhysicalMemory = ( long ) totalMethod.invoke( osBean );
			freePhysicalMemory = ( long ) freeMethod.invoke( osBean );
		} catch ( Exception e ) {
			try {
				Method totalMethod = osBean.getClass().getMethod("getTotalPhysicalMemorySize");
				Method freeMethod = osBean.getClass().getMethod("getFreePhysicalMemorySize");
				totalPhysicalMemory = (long) totalMethod.invoke(osBean);
				freePhysicalMemory = (long) freeMethod.invoke(osBean);
			} catch (Exception ex) {
				System.err.println("OS 메모리 정보를 가져올 수 없습니다.");
			}
		}

		long usedPhysicalMemory = totalPhysicalMemory - freePhysicalMemory;

		double totalMemoryGb = ( double ) totalPhysicalMemory / ( 1024 * 1024 * 1024 );
		double usedMemoryGb = ( double ) usedPhysicalMemory / ( 1024 * 1024 * 1024 );
		double memoryUsagePercent = totalPhysicalMemory > 0 ? ( ( double ) usedPhysicalMemory / totalPhysicalMemory ) * 100 : 0.0;

		metrics.put("totalMemory", String.format("%.2f", totalMemoryGb));
		metrics.put("usedMemory", String.format("%.2f", usedMemoryGb));
		metrics.put("memoryPercent", String.format("%.2f", memoryUsagePercent));

		Map<String, String> gpuData = getGpuMetricsFromNvidiaSmi();
		metrics.putAll( gpuData );

		return metrics;
	}

	public Map<String, String> getGpuMetricsFromNvidiaSmi() {
		Map<String, String> gpuMetrics = new HashMap<>();

		gpuMetrics.put( "gpu3d", "0" );
		gpuMetrics.put( "gpuDedicatedUsed", "0" );
		gpuMetrics.put( "gpuDedicatedTotal", "0" );
		gpuMetrics.put( "gpuSharedUsed", "0" );

		try {
			Process process = Runtime.getRuntime().exec(
				"nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits"
			);

			BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
			String line = reader.readLine();

			if ( line != null ) {
				String[] tokens = line.split( "," );
				if ( tokens.length >= 3 ) {
					gpuMetrics.put( "gpu3d", tokens[0].trim() );
					gpuMetrics.put( "gpuDedicatedUsed", tokens[1].trim() );
					gpuMetrics.put( "gpuDedicatedTotal", tokens[2].trim() );

					int mockSharedUsed = ( int ) ( Integer.parseInt( tokens[0].trim() ) * 0.4 );
					gpuMetrics.put( "gpuSharedUsed", String.valueOf( mockSharedUsed ) );
				}
			}
			reader.close();
		} catch ( Exception e ) {
			System.err.println( "NVIDIA GPU 정보를 가져오는데 실패했습니다: " + e.getMessage() );
		}

		return gpuMetrics;
	}
}
