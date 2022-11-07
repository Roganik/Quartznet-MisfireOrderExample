using System.Collections.Specialized;
using System.Reflection;
using Quartz;
using Quartz.Impl;

namespace Quartznet_MisfireOrderExample_Lib;

internal static class ConfigProvider
{
    public static NameValueCollection GetConfig()
    {
        var misfireThresholdMilliseconds = 1 * 1000; // 1 second

        return new NameValueCollection()
        {
            { "quartz.threadPool.threadCount", "1" },
            { "quartz.jobStore.misfireThreshold", misfireThresholdMilliseconds.ToString() },
            { "quartz.scheduler.instanceName", "MyProjQuartzScheduler" } /* This name used to find QuartzSchedule */
        };
    }
}

 internal sealed class QuartzScheduler
 {
        private IScheduler _scheduler;

        public QuartzScheduler()
        {
            var properties = ConfigProvider.GetConfig();
            var schedulerFactory = new StdSchedulerFactory(properties);
            _scheduler = schedulerFactory.GetScheduler().Result;
        }
        
        internal void Start()
        {
            _scheduler.Start();
        }

        internal void Stop()
        {
            _scheduler.Shutdown(true);
        }

        internal void Clear()
        {
            _scheduler.Clear();
        }

        internal void ScheduleJob(Schedule job)
        {
            System.Diagnostics.Debug.WriteLine("Scheduling Job");
            
            var quartzJob = JobBuilder.Create<JobAdapter>()
                .WithIdentity(job.JobName)
                .Build();

            /* Note: The default misfire handling policy is applied.
               Immediately executes first misfired execution and discards other (i.e. all misfired executions are merged together).
               Then back to schedule. No matter how many trigger executions were missed, only single immediate execution is performed.
               If more than one different jobs misfired, they are executed in order of priority.  */
            var trigger = TriggerBuilder.Create()
                .WithIdentity($"{job.JobName}Trigger")
                .ForJob(quartzJob.Key)
                .WithSchedule(CronScheduleBuilder.CronSchedule(job.Cron))
                .WithPriority(job.Priority)
                .Build();

            _scheduler.ScheduleJob(quartzJob, trigger);
        }

        internal ITrigger GetTrigger(TriggerKey triggerKey)
        {
            return _scheduler.GetTrigger(triggerKey).Result;
        }
    }

public sealed class Schedule
{
    public string JobName { get; set; }

    public string Cron { get; set; }

    public int Priority { get; set; }
}
    
    