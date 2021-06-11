package com.mgg.schedule;

import com.mgg.schedule.message.Work;
import com.mgg.schedule.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redisson.Redisson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
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

}
