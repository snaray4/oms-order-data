package com.yantriks.statistics.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.json.JSONObject;

import com.yantra.yfc.statistics.interfaces.IStatisticObject;
import com.yantra.yfc.statistics.interfaces.PLTStatisticsConsumer;

/**
 * This class implements PLTStatisticsConsumer interface . So that Statistics
 * data can be sent to an external system instead of Oracle DB
 *
 * @author Yantriks
 */

public class PublishStatisticsToExternalSystem implements PLTStatisticsConsumer {

	// private static final YFCLogCategory logger =
	// YFCLogCategory.instance(PublishStatisticsToExternalSystem.class.getName());

	@Override
	public void consumeStatistics(Collection<IStatisticObject> stats) {

		//System.out.println(stats);

		if (stats.isEmpty()) {
			System.out.println("collection length is 0");
			return; // No data to work with
		}
		Map<String, List<IStatisticObject>> iResponseTimeStatisticObjectMap = new HashMap<>();
		// Copy the unmodifiable collection to a modifiable one
		Collection<IStatisticObject> statCol = new ArrayList<>(stats);
		//System.out.println("Collection in array:" + statCol);
		Iterator<IStatisticObject> iter = statCol.iterator();
		while (iter.hasNext()) {
			IStatisticObject statObj = iter.next();
			if ("ResponseTime".equals(statObj.getStatName())) {
				System.out.println("statName is ResponseTime");
				getStatistics(iResponseTimeStatisticObjectMap, iter, statObj);
			}
		}

		//System.out.println("iResponseTimeStatisticObjectMap::" + iResponseTimeStatisticObjectMap);

		JSONObject jsson = new JSONObject(iResponseTimeStatisticObjectMap);

		System.out.println("" + jsson);

		KafkaProd kafka = new KafkaProd("order_statistics");
		kafka.sendMsgToKafka(jsson, kafka);

		System.out.println("End of class");

	}

	private void getStatistics(Map<String, List<IStatisticObject>> iStatisticObjectMap, Iterator<IStatisticObject> iter,
			IStatisticObject statObj) {

		List<IStatisticObject> iStatisticObjectList;
		String sStatObjKey = statObj.getContextName() + statObj.getHostName() + statObj.getServerId()
				+ statObj.getServerName() + statObj.getServiceName() + statObj.getServiceType() + statObj.getStatName();

		if (iStatisticObjectMap.containsKey(sStatObjKey)) {

			iStatisticObjectList = iStatisticObjectMap.get(sStatObjKey);
			iStatisticObjectList.add(statObj);
		} else {

			iStatisticObjectList = new ArrayList<>();
			iStatisticObjectList.add(statObj);
			iStatisticObjectMap.put(sStatObjKey, iStatisticObjectList);
		}
		iter.remove();

	}

}