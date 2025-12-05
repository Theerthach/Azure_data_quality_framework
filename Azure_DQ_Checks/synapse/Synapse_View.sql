

SELECT * FROM sys.external_data_sources;

/*
ALTER EXTERNAL DATA SOURCE DQStorage
WITH (
    LOCATION = 'https://dqcheckstheertha.dfs.core.windows.net',
    CREDENTIAL = SynapseIdentity
);
GO
*/

CREATE EXTERNAL DATA SOURCE DQStorage
WITH (
    LOCATION = 'https://dqcheckstheertha.dfs.core.windows.net',
    CREDENTIAL = SynapseIdentity
);
GO

CREATE OR ALTER VIEW dq_results_view AS
SELECT *
FROM OPENROWSET(
        BULK '/silver/dq_results/',
        DATA_SOURCE = 'DQStorage',
        FORMAT = 'DELTA'
     ) AS dq;
GO