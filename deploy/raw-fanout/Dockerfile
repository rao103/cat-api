# Supply your approved, pinned JFrog SDK and runtime images as build arguments.
ARG SDK_IMAGE
ARG RUNTIME_IMAGE
FROM ${SDK_IMAGE} AS build
WORKDIR /source
COPY . .
RUN dotnet publish src/Raw/FanOut/RawFanOut.Worker/RawFanOut.Worker.csproj -c Release -o /out
FROM ${RUNTIME_IMAGE}
WORKDIR /app
COPY --from=build /out .
USER 1654
ENTRYPOINT ["dotnet", "RawFanOut.Worker.dll"]
