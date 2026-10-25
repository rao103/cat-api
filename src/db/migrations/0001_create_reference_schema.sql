-- Reference data shared by every ingestor: windfarms and their turbines.
-- Fresh-database script. Apply 0001, 0002, 0003 in order, then the files in PostDeployment/.
-- Table names are intentionally unqualified so they land in the connection's default schema (public).

CREATE TABLE windfarm (
    id                      INTEGER          NOT NULL,
    windfarm_name           TEXT             NOT NULL,
    windfarm_name_in_source TEXT,
    time_offset             DOUBLE PRECISION NOT NULL DEFAULT 0,
    min_retrieve_date       TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT TIMESTAMP '2016-01-01 00:00:00',
    is_active               BOOLEAN          NOT NULL DEFAULT FALSE,

    CONSTRAINT windfarm_pkey PRIMARY KEY (id)
);

CREATE TABLE wind_turbine (
    id          INTEGER    NOT NULL,
    wtg_name    VARCHAR(8) NOT NULL,
    windfarm_id INTEGER    NOT NULL,
    turbine_toc TIMESTAMP WITHOUT TIME ZONE,
    -- Decommissioned turbines are excluded from ProductionChanges scope planning.
    is_active   BOOLEAN    NOT NULL DEFAULT TRUE,

    CONSTRAINT pk_wind_turbine PRIMARY KEY (id),
    CONSTRAINT fk_wind_turbine_windfarm FOREIGN KEY (windfarm_id) REFERENCES windfarm (id)
);
