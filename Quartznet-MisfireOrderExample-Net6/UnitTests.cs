using NUnit.Framework;
using Quartznet_MisfireOrderExample_Lib;

namespace Quartznet_MisfireOrderExample_Net6;

public class Tests
{
    [TestFixture]
    public sealed class UnitTests
    {
        [Test]
        public async Task Scenario1()
        {
            var lib = new QuartzSchedulerPriorityTests();
            await lib.Scenario1();
        }
        
        [Test]
        public async Task Scenario2()
        {
            var lib = new QuartzSchedulerPriorityTests();
            await lib.Scenario2();
        }
    }
}