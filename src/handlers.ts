import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import AWS from "aws-sdk";
import { v4 } from "uuid";
import * as yup from "yup";

const docClient = new AWS.DynamoDB.DocumentClient();
const tableName = "CountriesTable";
const nighbortableName = "NeighborsTable";
const headers = {
  "content-type": "application/json",
};

const schema = yup.object().shape({
  name: yup.string().required(),
  description: yup.string().required(),
  currency: yup.string().required(),
  capital: yup.string().required(),
  region: yup.string().required(),
  subregion: yup.string().required(),
  area: yup.number().required(),
  population: yup.number().required(),
  flag_url: yup.string().required(),
  neighbors: yup.array().of(yup.string()),
  created_at: yup.date().default(() => new Date()),
  updated_at: yup.date().default(() => new Date()),
});

const neighborSchema = yup.object().shape({
  countryId: yup.string().required(),
  neighborId: yup.string().required(),
  createdAt: yup.date().default(() => new Date()),
});

export const createCountry = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const requestBody = JSON.parse(event.body as string);

    if (!Array.isArray(requestBody)) {
      throw new Error("Request body should be an array of countries.");
    }

    const countries = requestBody.map((country: any) => ({
      ...country,
      countryID: v4(),
    }));

    const putRequests = countries.map((country: any) => ({
      PutRequest: {
        Item: country,
      },
    }));

    const batchWriteParams = {
      RequestItems: {
        [tableName]: putRequests,
      },
    };

    await docClient.batchWrite(batchWriteParams).promise();

    return {
      statusCode: 201,
      headers,
      body: JSON.stringify(countries),
    };
  } catch (error) {
    return handleError(error);
  }
};

class HttpError extends Error {
  constructor(public statusCode: number, body: Record<string, unknown> = {}) {
    super(JSON.stringify(body));
  }
}

const handleError = (e: unknown) => {
  if (e instanceof yup.ValidationError) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({
        errors: e.errors,
      }),
    };
  }

  if (e instanceof SyntaxError) {
    return {
      statusCode: 400,
      headers,
      body: JSON.stringify({ error: `invalid request body format : "${e.message}"` }),
    };
  }

  if (e instanceof HttpError) {
    return {
      statusCode: e.statusCode,
      headers,
      body: e.message,
    };
  }

  throw e;
};

export const getCountry = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const country = await fetchCountryById(event.pathParameters?.id as string);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(country),
    };
  } catch (e) {
    return handleError(e);
  }
};

export const updateCountry = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const id = event.pathParameters?.id as string;

    await fetchCountryById(id);

    const reqBody = JSON.parse(event.body as string);

    await schema.validate(reqBody, { abortEarly: false });

    const country = {
      ...reqBody,
      countryID: id,
    };

    await docClient
      .put({
        TableName: tableName,
        Item: country,
      })
      .promise();

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(country),
    };
  } catch (e) {
    return handleError(e);
  }
};

export const deleteCountry = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const id = event.pathParameters?.id as string;

    await fetchCountryById(id);

    await docClient
      .delete({
        TableName: tableName,
        Key: {
          countryID: id,
        },
      })
      .promise();

    return {
      statusCode: 204,
      body: "",
    };
  } catch (e) {
    return handleError(e);
  }
};

export const listCountry = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  const output = await docClient
    .scan({
      TableName: tableName,
    })
    .promise();

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify(output.Items),
  };
};

export const addNeighbors = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const { countryId } = event.pathParameters || {};
    const neighborData: string[] = JSON.parse(event.body as string);

    // Ensure that the country exists
    const country = await fetchCountryById(countryId as string);
    if (!country) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ message: "Country not found", data: {} }),
      };
    }

    // Get existing country IDs to validate neighbors
    const existingCountryIds = await fetchAllCountryIds();

    const errors: string[] = [];
    const successfulAdditions: string[] = [];

    // Iterate through neighbor data
    for (const neighborId of neighborData) {
      // Validate neighbor ID
      if (!existingCountryIds.includes(neighborId)) {
        errors.push(`Invalid neighbor country ID: ${neighborId}`);
        continue;
      }

      // Check if neighbor already exists for the country
      const existingNeighbor = await fetchNeighbor(countryId as string, neighborId);
      if (existingNeighbor) {
        errors.push(`Neighbor with ID ${neighborId} already exists for this country`);
        continue;
      }

      // Add neighbor to NeighborsTable
      await addNeighbor(countryId as string, neighborId);
      successfulAdditions.push(neighborId);
    }

    // Return response
    if (successfulAdditions.length === 0) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ message: "Failed to add neighbors", data: { neighbors: [], errors } }),
      };
    } else {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          message: "Neighbors added successfully",
          data: { neighbors: successfulAdditions },
          errors,
        }),
      };
    }
  } catch (error: any) {
    console.error(error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
    };
  }
};

const fetchAllCountryIds = async (): Promise<string[]> => {
  const result = await docClient
    .scan({
      TableName: tableName,
      ProjectionExpression: "countryID",
    })
    .promise();

  return result.Items ? result.Items.map((item) => item.countryID) : [];
};

const fetchNeighbor = async (countryId: string, neighborId: string): Promise<any> => {
  const output = await docClient
    .get({
      TableName: nighbortableName,
      Key: {
        countryId: countryId,
        neighborId: neighborId,
      },
    })
    .promise();

  return output.Item;
};

const addNeighbor = async (countryId: string, neighborId: string): Promise<void> => {
  await docClient
    .put({
      TableName: nighbortableName,
      Item: {
        countryId: countryId,
        neighborId: neighborId,
      },
    })
    .promise();
};

export const getCountryNeighbors = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const { countryId } = event.pathParameters || {};

    // Retrieve the country from the CountriesTable
    const country = await fetchCountryById(countryId as string);

    if (!country) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ message: "Country not found", data: {} }),
      };
    }

    // Retrieve neighbors from the NeighborsTable
    const neighbors = await fetchNeighborsByCountryId(countryId as string);

    if (neighbors.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ message: "Country neighbours", data: { countries: [] } }),
      };
    } else {
      const neighborCountries = await Promise.all(
        neighbors.map(async (neighborId) => {
          const neighbor = await fetchCountryById(neighborId);
          return {
            id: neighbor.countryID,
            name: neighbor.name,
            cca3: neighbor.cca3,
            currency_code: neighbor.currency_code,
            currency: neighbor.currency,
            capital: neighbor.capital,
            region: neighbor.region,
            subregion: neighbor.subregion,
            area: neighbor.area,
            map_url: neighbor.map_url,
            population: neighbor.population,
            flag_url: neighbor.flag_url,
          };
        }),
      );

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ message: "Country neighbours", data: { countries: neighborCountries } }),
      };
    }
  } catch (error: any) {
    console.error(error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
    };
  }
};

const fetchCountryById = async (id: string): Promise<any> => {
  const output = await docClient
    .get({
      TableName: tableName,
      Key: {
        countryID: id,
      },
    })
    .promise();

  return output.Item;
};

const fetchNeighborsByCountryId = async (countryId: string): Promise<string[]> => {
  const result = await docClient
    .query({
      TableName: nighbortableName,
      KeyConditionExpression: "countryId = :countryId",
      ExpressionAttributeValues: {
        ":countryId": countryId,
      },
    })
    .promise();

  return result.Items ? result.Items.map((item) => item.neighborId) : [];
};

export const getAllCountriesPaginated = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    let { page = "1", limit = "10", sort_by = "a_to_z", search } = event.queryStringParameters || {};
    let skip = (parseInt(page) - 1) * parseInt(limit);
    let sortCriteria: any = {};

    switch (sort_by) {
      case "a_to_z":
        sortCriteria = { name: 1 };
        break;
      case "z_to_a":
        sortCriteria = { name: -1 };
        break;
      case "population_high_to_low":
        sortCriteria = { population: -1 };
        break;
      case "population_low_to_high":
        sortCriteria = { population: 1 };
        break;
      case "area_high_to_low":
        sortCriteria = { area: -1 };
        break;
      case "area_low_to_high":
        sortCriteria = { area: 1 };
        break;
      default:
        sortCriteria = { name: 1 };
        break;
    }

    let query: any = {};
    if (search) {
      const searchRegex = new RegExp(search, "i");
      query = {
        $or: [{ name: searchRegex }, { region: searchRegex }, { subregion: searchRegex }],
      };
    }

    // Validation of query parameters using Yup schemas
    await yup
      .object()
      .shape({
        page: yup.string().default("1"),
        limit: yup.string().default("10"),
        sort_by: yup
          .string()
          .oneOf([
            "a_to_z",
            "z_to_a",
            "population_high_to_low",
            "population_low_to_high",
            "area_high_to_low",
            "area_low_to_high",
          ])
          .default("a_to_z"),
        search: yup.string().default(""),
      })
      .validate({
        page,
        limit,
        sort_by,
        search,
      });

    // Example data retrieval, replace it with actual data from your database
    const totalCountries = 100; // Example total count, replace this with your actual count
    const totalPages = Math.ceil(totalCountries / parseInt(limit));

    const countries: any = []; // Example array, replace this with your actual data retrieval

    const hasNext = parseInt(page) < totalPages;
    const hasPrev = parseInt(page) > 1;

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Country list",
        data: {
          list: countries,
          has_next: hasNext,
          has_prev: hasPrev,
          page: parseInt(page),
          pages: totalPages,
          per_page: parseInt(limit),
          total: totalCountries,
        },
      }),
    };
  } catch (error) {
    console.error("Error fetching countries:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Internal Server Error", data: {} }),
    };
  }
};
