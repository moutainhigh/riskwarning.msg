package com.hncy58.spark2.dbscan.invoker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hncy58.spark.dbscan.spatial.Point;

public class MsgSendInvoker implements Invoker<List<Point>, Boolean> {

	@Override
	public Boolean invoke(List<Point> data) throws Exception {

		final Map<Long, List<Point>> points = new HashMap<Long, List<Point>>();
		data.forEach(p -> {
			if (points.containsKey(p.clusterId())) {
				points.get(p.clusterId()).add(p);
			} else {
				List<Point> l = new ArrayList<Point>();
				l.add(p);
				points.put(p.clusterId(), l);
			}
		});

		for (Entry<Long, List<Point>> entry : points.entrySet()) {
			System.out.println(entry.getKey() + " size " + entry.getValue().size());
		}

		return true;
	}

}
