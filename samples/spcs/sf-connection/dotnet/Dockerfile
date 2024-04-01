FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /App

# Copy project files for build
COPY App.cs ./
COPY app.config ./
COPY Snowflake.App.csproj ./
# Restore as distinct layers
RUN dotnet restore
# Build and publish a release
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /App

COPY --from=build-env /App/out .
COPY log4net.config /App/log4net.config
CMD ["dotnet", "Snowflake.App.dll"]