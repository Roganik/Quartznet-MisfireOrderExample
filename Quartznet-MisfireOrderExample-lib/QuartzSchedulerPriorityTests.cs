using Quartz;

namespace Quartznet_MisfireOrderExample_Lib;


public sealed class QuartzSchedulerPriorityTests
{
    private const int StandardPriority = 0;
    private const int HighPriority = 1;
    
    public async Task Scenario1()
    {
        // Scenario 1
        // Long-running (blocking) job is run (at 00:00)
        // While it's being run other 3 jobs should be run : job1 (at 00:05), job2 (at 00:10), job3 (at 00:15)
        // job2 has higher priority.
        // When blocking job is finished (at 00:20), the other 3 are run one by one.
        // Expected behaviour: job2 must be run before job1 and job3, even though it's scheduled after them.
        // Note: high priority is assigned to job2 rather than job3, because job3 is scheduled last,
        // and that might be a special position in scheduling logic.
        
        // Arrange
        var quartzScheduler = new QuartzScheduler(); 
        var schedulerRunTime = DateTime.Now.AddSeconds(10); // Give some time for arranging test data before Act stage

        var blockingJobScheduledTime = schedulerRunTime;
        var blockingJobTriggerKey = ScheduleJob(BlockingTestMyJob.JobName, blockingJobScheduledTime, StandardPriority, quartzScheduler);

        var job1ScheduledTime = schedulerRunTime.AddSeconds(5);
        var job1TriggerKey = ScheduleJob(TestMyJob1.JobName, job1ScheduledTime, StandardPriority, quartzScheduler);

        var job2ScheduledTime = schedulerRunTime.AddSeconds(10);
        var job2TriggerKey = ScheduleJob(TestMyJob2.JobName, job2ScheduledTime, HighPriority, quartzScheduler);

        var job3ScheduledTime = schedulerRunTime.AddSeconds(15);
        var job3TriggerKey = ScheduleJob(TestMyJob3.JobName, job3ScheduledTime, StandardPriority, quartzScheduler);

        // Act
        quartzScheduler.Start();
        await WaitUntil(schedulerRunTime.AddSeconds(30));

        // Assert
        var blockingJobRunTime = GetJobRunTime(quartzScheduler, blockingJobTriggerKey);
        var job1RunTime = GetJobRunTime(quartzScheduler, job1TriggerKey);
        var job2RunTime = GetJobRunTime(quartzScheduler, job2TriggerKey);
        var job3RunTime = GetJobRunTime(quartzScheduler, job3TriggerKey);

        quartzScheduler.Clear();

        Assert.IsNotNull(blockingJobRunTime, "blockingJob must be run");
        Assert.IsNotNull(job1RunTime, "job1 must be run");
        Assert.IsNotNull(job2RunTime, "job2 must be run");
        Assert.IsNotNull(job3RunTime, "job3 must be run");
        Assert.IsTrue(job2RunTime < job1RunTime, "job2 must be run before job1");
        Assert.IsTrue(job2RunTime < job3RunTime, "job2 must be run before job3");
    }
    
    public async Task Scenario2()
    {
        // Scenario 2
        // Long-running (blocking) job is run (at 00:00)
        // While it's being run other 2 jobs should be run : job4 (at 00:05), job5 (at 00:10)
        // When blocking job is finished (at 00:20), the other 2 is run. job4 runs first.
        // While it's being run another job should be run : job3 (at 00:25).
        // job3 has higher priority.
        // When job4 is finished (at 00:30), the remaining jobs are run.
        // Expected behaviour: job3 must be run before job5, even though job5 misfired and was re-scheduled before job3.

        // Arrange
        var quartzScheduler = new QuartzScheduler();
        var schedulerRunTime = DateTime.Now.AddSeconds(10); // Give some time for arranging test data before Act stage

        var blockingJobScheduledTime = schedulerRunTime;
        var blockingJobTriggerKey = ScheduleJob(BlockingTestMyJob.JobName, blockingJobScheduledTime, StandardPriority, quartzScheduler);

        var job4ScheduledTime = schedulerRunTime.AddSeconds(5);
        var job4TriggerKey = ScheduleJob(TestMyJob4.JobName, job4ScheduledTime, StandardPriority, quartzScheduler);

        var job5ScheduledTime = schedulerRunTime.AddSeconds(10);
        var job5TriggerKey = ScheduleJob(TestMyJob5.JobName, job5ScheduledTime, StandardPriority, quartzScheduler);

        var job3ScheduledTime = schedulerRunTime.AddSeconds(25);
        var job3TriggerKey = ScheduleJob(TestMyJob3.JobName, job3ScheduledTime, HighPriority, quartzScheduler);

        // Act
        quartzScheduler.Start();
        await WaitUntil(schedulerRunTime.AddSeconds(40));

        // Assert
        var blockingJobRunTime = GetJobRunTime(quartzScheduler, blockingJobTriggerKey);
        var job3RunTime = GetJobRunTime(quartzScheduler, job3TriggerKey);
        var job4RunTime = GetJobRunTime(quartzScheduler, job4TriggerKey);
        var job5RunTime = GetJobRunTime(quartzScheduler, job5TriggerKey);
        
        quartzScheduler.Clear();

        Assert.IsNotNull(blockingJobRunTime, "blockingJob must be run");
        Assert.IsNotNull(job4RunTime, "job4 must be run");
        Assert.IsNotNull(job5RunTime, "job5 must be run");
        Assert.IsNotNull(job3RunTime, "job3 must be run");
        Assert.IsTrue(job3RunTime < job5RunTime, "job3 must be run before job2.");
    }

    /// <summary>
    /// Schedules job and returns trigger key for it
    /// </summary>
    private TriggerKey ScheduleJob(string jobName, DateTime jobScheduledTime, int priority, QuartzScheduler quartzScheduler)
    {
        var jobSchedule = new Schedule
        {
            JobName = jobName,
            Cron = DateTimeToCron(jobScheduledTime),
            Priority = priority
        };

        TestContext.Progress.WriteLine($"ScheduleJob -> {jobSchedule.JobName, 15} to run at {jobSchedule.Cron} with Priority {jobSchedule.Priority}");
        
        quartzScheduler.ScheduleJob(jobSchedule);

        var triggerName = $"{jobName}Trigger";
        var triggerGroup = "DEFAULT";
        return new TriggerKey(triggerName, triggerGroup);
    }

    private static string DateTimeToCron(DateTime dateTime)
    {
        return $"{dateTime.Second} {dateTime.Minute} {dateTime.Hour} ? * *";
    }

    private static DateTimeOffset? GetJobRunTime(QuartzScheduler quartzScheduler, TriggerKey jobTriggerKey)
    {
        var jobTrigger = quartzScheduler.GetTrigger(jobTriggerKey);
        return jobTrigger.GetPreviousFireTimeUtc();
    }

    private async Task WaitUntil(DateTime finishDateTime)
    {
        var resolveInSeconds = 1000 * (finishDateTime - DateTime.Now).Seconds;
        await Task.Delay(resolveInSeconds);
    }

    #region Test jobs

    private abstract class TestMyJobBase : MyJob
    {
        public TestMyJobBase()
        {
            TestContext.Progress.WriteLine($"{this.GetType().Name} created. Job Execute Time is {ExecuteTimeInSeconds} secs.");
        }
        
        public virtual int ExecuteTimeInSeconds => 2; // quartz.jobStore.misfireThreshold * 2
        
        public override void Execute(CancellationToken? token = null)
        {
            var dtCronLike = (DateTime dt) => $"{dt.Second, 2} {dt.Minute, 2} {dt.Hour,2}";
            TestContext.Progress.WriteLine($"Starting at {dtCronLike(DateTime.Now)}");
            var startTime = DateTime.Now;
            do
            {
                // Nothing
            }
            while (DateTime.Now < startTime.AddSeconds(ExecuteTimeInSeconds));
            
            TestContext.Progress.WriteLine($"Executed at {dtCronLike(DateTime.Now)}");
        }
    }

    [JobAttribute(JobName)]
    private sealed class BlockingTestMyJob : TestMyJobBase
    {
        public const string JobName = "BlockingTestJob";

        public override int ExecuteTimeInSeconds => 20;
    }

    [JobAttribute(JobName)]
    private sealed class TestMyJob1 : TestMyJobBase
    {
        public const string JobName = "TestJob1";
    }

    [JobAttribute(JobName)]
    private sealed class TestMyJob2 : TestMyJobBase
    {
        public const string JobName = "TestJob2";
    }

    [JobAttribute(JobName)]
    private sealed class TestMyJob3 : TestMyJobBase
    {
        public const string JobName = "TestJob3";
    }

    [JobAttribute(JobName)]
    private sealed class TestMyJob4 : TestMyJobBase
    {
        public const string JobName = "TestJob4";

        public override int ExecuteTimeInSeconds => 10;
    }

    [JobAttribute(JobName)]
    private sealed class TestMyJob5 : TestMyJobBase
    {
        public const string JobName = "TestJob5";
    }

    #endregion
}
