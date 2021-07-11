package com.mgg.schedule;

import com.mgg.schedule.message.Work;
import com.mgg.schedule.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ScheduleApplication.class)
public class ScheduleApplicationTests {

	@Autowired
	Redisson redisson;

	@Autowired
	Producer producer;

	@Test
	public void prowork() {
		Work work = new Work(1L,100d);
		Work work1 = new Work(2L,101d);
		Work work8 = new Work(8L,102d);
		Work work9 = new Work(9L,103d);
		Work work10 = new Work(10L,104d);
		List<Work> workList = new ArrayList<>();
		workList.add(work);
		workList.add(work1);
		workList.add(work8);
		workList.add(work9);
		workList.add(work10);
		producer.prowork(workList);
	}


	@Test
	public void storeToPre() {
		/**
		 * 消息开始消费，每次拉取1条信息进行消费。由storeQueue将任务迁移到->prepareQueu中。
		 *
		 * 这里的分支-》消费成功 -》删除prepareQueue中的任务
		 *
		 * 消费失败 -》进行任务的回滚-》prepareQueue-》 storeQueue。
		 *
		 * 消费者异常宕机 -》消费者异常比如宕机或重启后。prepareQueue中存在遗留任务。
		 *
		 * 这里只需要消费者在重启后重新开始消费任务后，在最后的逻辑去检查下prepareQueue中是否存在小于当前时间戳的任务。若存在就将其
		 * 回滚至storeQueue中。
		 *
		 */

		String script = "local expiredValues = redis.call('ZRANGEBYSCORE',KEYS[1],0,ARGV[1],'WITHSCORES','limit',0,ARGV[2]); " +
				"local res = {}; " +
				"if #expiredValues > 0 then " +
					"for i,v in ipairs(expiredValues) do " +
						"if (i % 2 == 1) then " +
							"table.insert(res,v) " +
						"elseif(i % 2 == 0) then " +
							"if(tonumber(v) >= 16) then " +
								"redis.call('zadd',KEYS[2],(16 + ARGV[3] * 1000),res[i / 2]) " +
							"elseif(tonumber(v) > 0) then redis.call('zadd',KEYS[2],(v + ARGV[3] * 1000),res[i / 2]) " +
							//"else TODO 进入死信队列" +
							"end; " +
							"redis.call('zrem',KEYS[1],res[i / 2])" +
						"end; " +
					"end; " +
					//"redis.call('ZREMRANGEBYSCORE',KEY[1],0,ARGV[1]) " +
				"end; " +
				"return res;";
		List<Object> keys = new ArrayList<>();
		keys.add("mgg_topic_1");
		keys.add("mgg_topic_0");

		long mii = System.currentTimeMillis();
		long second = mii / 1000L;
		Object map =  redisson.getScript().eval(RScript.Mode.READ_WRITE,script,RScript.ReturnType.MAPVALUELIST,keys,Double.valueOf(mii),100,Double.valueOf(second));
		System.out.println(map);

	}



	@Test
	public void preReturnBackToStore() {
		/***
		 * prepareQueue任务回滚-至storeQueue。
		 */
		String script = "local expiredValues = redis.call('ZRANGEBYSCORE',KEYS[1],0,ARGV[1],'WITHSCORES','limit',0,ARGV[2]); " +
				"local res = {}; " +
				"if #expiredValues > 0 then " +
					"for i,v in ipairs(expiredValues) do " +
						"if (i % 2 == 1) then " +
							"table.insert(res,v) " +
						"elseif(i % 2 == 0) then " +
//							"if(tonumber(v) >= 16) then " +
//							//	"print(16 + ARGV[3] * 1000) print(v % 10)" +
							"redis.call('zadd',KEYS[2],(v % 1000 - 1),res[i / 2]) " +
//							"else redis.call('zadd',KEYS[2],(v + ARGV[3] * 1000),res[i / 2]) " +
							"redis.call('zrem',KEYS[1],res[i / 2])" +
						"end; " +
					"end; " +
				//"redis.call('ZREMRANGEBYSCORE',KEY[1],0,ARGV[1]) " +
				"end; " +
				"return res;";
		List<Object> keys = new ArrayList<>();
		keys.add("mgg_topic_0");
		keys.add("mgg_topic_1");

		long mii = System.currentTimeMillis();
		long second = mii / 1000L;
		Object map =  redisson.getScript().eval(RScript.Mode.READ_WRITE,script,RScript.ReturnType.MAPVALUELIST,keys,Double.valueOf(mii),100,Double.valueOf(second));
		System.out.println(map);

	}

	@Test
	public void test3() {
		String script = "local t = redis.call('ZRANGEBYSCORE','mgg_topic_0',0,60000,'WITHSCORES','limit',0,100); " +
				" local res = {} for i,v in pairs(t) do if( i % 2 == 1) then table.insert(res,v) else print(res[i / 2]) end;  end;" +
				//" for i,v in pairs(res) do print(i .. '__' .. v) end ; " +
				"return res;";
		List<Object> keys = new ArrayList<>();
		Object obj = redisson.getScript().eval(RScript.Mode.READ_WRITE,script,RScript.ReturnType.MAPVALUELIST);
		System.out.println(obj);

	}


	@Test
	public void test4() {
		Calendar calendar = Calendar.getInstance();
		System.out.println(calendar.SECOND);
	}



}
