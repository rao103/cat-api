# Build with the service-repository root as context and approved, pinned base images.
ARG SDK_IMAGE
ARG RUNTIME_IMAGE
FROM ${SDK_IMAGE} AS build
WORKDIR /source
COPY . .
RUN dotnet restore src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj --locked-mode
RUN dotnet publish src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj -c Release --no-restore -o /out
FROM ${RUNTIME_IMAGE}
WORKDIR /app
COPY --from=build /out .
USER 1654
ENTRYPOINT ["dotnet", "Harmonizer.Worker.dll"]
