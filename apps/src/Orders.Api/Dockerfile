FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props global.json ./
COPY apps/src/BuildingBlocks/BuildingBlocks.csproj apps/src/BuildingBlocks/
COPY apps/src/Orders.Domain/Orders.Domain.csproj apps/src/Orders.Domain/
COPY apps/src/Orders.Application/Orders.Application.csproj apps/src/Orders.Application/
COPY apps/src/Orders.Infrastructure/Orders.Infrastructure.csproj apps/src/Orders.Infrastructure/
COPY apps/src/Orders.Api/Orders.Api.csproj apps/src/Orders.Api/
RUN dotnet restore apps/src/Orders.Api/Orders.Api.csproj

COPY apps/src/BuildingBlocks/ apps/src/BuildingBlocks/
COPY apps/src/Orders.Domain/ apps/src/Orders.Domain/
COPY apps/src/Orders.Application/ apps/src/Orders.Application/
COPY apps/src/Orders.Infrastructure/ apps/src/Orders.Infrastructure/
COPY apps/src/Orders.Api/ apps/src/Orders.Api/
RUN dotnet publish apps/src/Orders.Api/Orders.Api.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build --chown=$APP_UID:$APP_UID /app/publish .

USER $APP_UID
ENV ASPNETCORE_URLS=http://+:8080 \
    DOTNET_EnableDiagnostics=0 \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
    OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
    OTEL_METRIC_EXPORT_INTERVAL=10000
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD curl --fail --silent http://127.0.0.1:8080/health/ready || exit 1

ENTRYPOINT ["dotnet", "Orders.Api.dll"]
