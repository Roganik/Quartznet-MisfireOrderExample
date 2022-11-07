using System.Reflection;
using Quartz;

namespace Quartznet_MisfireOrderExample_Lib;

public interface IMyJob
{
    void Execute(CancellationToken? token = null);
}

internal abstract class MyJob : IMyJob
{
    public abstract void Execute(CancellationToken? token = null);
}

public sealed class JobAdapter : Quartz.IJob
{
    public JobAdapter()
    {
    }

    public Task Execute(IJobExecutionContext context)
    {
        var jobName = context.JobDetail.Key.Name;
        
        var job = JobFactory.Create(jobName);
        job.Execute(context.CancellationToken);
        
        return Task.CompletedTask;
    }
}

[AttributeUsage(AttributeTargets.Class)]
public sealed class JobAttribute : Attribute
{
    public string JobName { get; }

    public JobAttribute(string jobName)
    {
        JobName = jobName;
    }
}

public static class JobFactory
{
    private static readonly IDictionary<string, Type> Cache;

    static JobFactory()
    {
        Cache = GetJobs(false);
    }

    public static IMyJob Create(string name)
    {
        if (!Cache.ContainsKey(name))
        {
            throw new ArgumentException($"Unknown job name: {name}");
        }

        var type = Cache[name];

        return Activator.CreateInstance(type) as IMyJob;
    }

    internal static IDictionary<string, Type> GetJobs(bool excludePrivate)
    {
        var result = new Dictionary<string, Type>();

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var types = assembly.GetTypes();
            var jobs = types.Where(t => t.GetInterfaces().Contains(typeof(IMyJob))).ToList();
            
            foreach (var type in jobs)
            {
                if (!type.GetInterfaces().Contains(typeof(IMyJob)) || type.IsAbstract || !type.IsClass || (excludePrivate && type.IsNestedPrivate))
                {
                    continue;
                }
                
                var jobName = type.GetAttribute<JobAttribute>().JobName;

                result.Add(jobName, type);
            }
        }

        return result;
    }
}

public static class ReflectionExtensions 
{
    public static TAttribute GetAttribute<TAttribute>(this MemberInfo memberInfo)
        where TAttribute : Attribute
    {
        return Attribute.GetCustomAttribute(memberInfo, typeof(TAttribute)) as TAttribute;
    }
    
    public static bool HasAttribute<T>(this object obj)
        where T : Attribute
    {
        if (obj == null)
        {
            return false;
        }

        return obj.HasAttribute(typeof(T));
    }
        
    public static bool HasAttribute(this object obj, Type attributeType)
    {
        if (obj == null)
        {
            return false;
        }
            
        return obj.GetType().GetCustomAttributes(attributeType, false).Any();
    }
}