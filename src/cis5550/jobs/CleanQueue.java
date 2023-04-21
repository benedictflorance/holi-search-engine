package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

public class CleanQueue {
	public static void run(FlameContext context, String[] args) throws Exception {
		FlameRDD urlQueue = context.fromTable("16820330347121774b860-0c7b-4f17-ae4c-bcd4da2e9ebd", row -> {
			if (row.get("value").contains("un")) {
				System.out.println("discard");
				return null;
			}
			System.out.println("keep");
			return row.get("value");
		});
		urlQueue.saveAsTable("queue");
		context.output("OK");
	}
}
