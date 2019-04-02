/*
 * Requires: https://github.com/RedHatInsights/insights-pipeline-lib
 */

@Library("github.com/RedHatInsights/insights-pipeline-lib") _


if (env.CHANGE_ID) {
    runSmokeTest (
        ocDeployerBuilderPath: "platform/upload-service",
        ocDeployerComponentPath: "platform/upload-service",
        ocDeployerServiceSets: "advisor,platform,platform-mq",
        iqePlugins: ["iqe-advisor-plugin"],
        pytestMarker: "advisor_smoke",
    )
}
