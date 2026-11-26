# logminer as a container: docker build -t logminer .
# The runtime image must have ICU and time zone data (SqlClient needs the one, log times are converted with the other): "chiseled" images only as "-extra".
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src ./src
RUN dotnet publish src/LogMiner.Service -c Release -o /app/logminer \
 && dotnet publish src/LogMiner.Verify -c Release -o /app/verify \
 && rm -f /app/logminer/appsettings.json

FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled-extra
# non-root, no shell, nothing is written (position, gaps and lease are in SQL Server): runs with a read-only root file system
WORKDIR /app/logminer
COPY --from=build /app /app
COPY infra/k8s/appsettings.json /app/logminer/appsettings.json
ENV DOTNET_EnableDiagnostics=0 LogMiner__HealthPort=8080
EXPOSE 8080
USER 1654
ENTRYPOINT ["dotnet", "/app/logminer/logminer.dll"]
# docker run --rm --entrypoint dotnet logminer /app/verify/logminer-verify.dll --stream LOGMINER --url nats://nats:4222
