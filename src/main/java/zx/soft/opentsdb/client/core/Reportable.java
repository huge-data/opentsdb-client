package zx.soft.opentsdb.client.core;

import java.util.List;

import zx.soft.opentsdb.metrics.core.OpenTsdbMetric;

/**
 * 报表接口
 *
 * @author wanggang
 *
 */
public interface Reportable {

	List<OpenTsdbMetric> report();

}
