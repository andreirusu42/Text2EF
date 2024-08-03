using entity_framework.Models.customers_and_addresses;
using Microsoft.EntityFrameworkCore;

class Program
{
    public static void Main()
    {
        var context = new CustomersAndAddressesContext();

        var sql = "SELECT country FROM addresses GROUP BY country HAVING count(address_id) > 4";
        var linq = context.Addresses.GroupBy(row => new { row.Country }).Where(group => group.Count(row => row.AddressId != null) > 4).Select(group => new { group.Key.Country }).ToList();

        Tester.Test(linq, sql, context);
    }

}