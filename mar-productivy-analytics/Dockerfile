FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY server/MAR.Productivy.Analytics.Server/ server/MAR.Productivy.Analytics.Server/
RUN dotnet publish server/MAR.Productivy.Analytics.Server/MAR.Productivy.Analytics.Server.csproj -c Release -o /app --no-self-contained

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "MAR.Productivy.Analytics.Server.dll"]
