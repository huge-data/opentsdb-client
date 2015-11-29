package zx.soft.opentsdb.reporter.simple;

import java.util.List;

import zx.soft.opentsdb.metric.OpenTsdbMetric;

/**
 * Report接口
 *
 * @author wanggang
 *
 */
public interface Reportable {

	List<OpenTsdbMetric> report();

}
