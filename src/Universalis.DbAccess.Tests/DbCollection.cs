using Xunit;

namespace Universalis.DbAccess.Tests
{
    [CollectionDefinition("Database collection")]
    public class DbCollection : ICollectionFixture<DbFixture>
    {
    }
}
