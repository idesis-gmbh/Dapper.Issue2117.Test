# Test case for Dapper issue #2117

## Installation

The script `create_db.sql`creates two tables `parent` and `child` in a Postgres schema of your choice and populates them with data. Please adjust `DEFAULT_CONNECTION` in `SetupBase.cs` accordingly.

## Running

To demonstrate the issue please run `dotnet test` in the project directory.
If you uncomment the `SYNC` define in `GenericRepository.cs` the problem vanishes.





