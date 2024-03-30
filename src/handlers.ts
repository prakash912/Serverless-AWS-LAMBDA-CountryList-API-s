import { APIGatewayProxyEvent, APIGatewayProxyResult } from "aws-lambda";
import AWS, { DynamoDB } from "aws-sdk";
import { v4 } from "uuid";
import * as yup from "yup";

// Set the region to the region where your DynamoDB table is located
AWS.config.update({ region: "ap-south-1" });

// Initialize DynamoDB Document Client
const docClient = new AWS.DynamoDB.DocumentClient();
const tableName = "CountriesTable";
const nighbortableName = "CountryNeighborsTable";
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
  countryID: yup.string().required(),
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
  } catch (error: any) {
    console.error("Error creating countries:", error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
    };
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

const schema2 = yup.array().of(yup.string());

export const addNeighbors = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    // Extract country ID from path parameters
    const { countryID } = event.pathParameters || {};

    // Parse request body
    const requestBody = JSON.parse(event.body as string);
    const neighborData: { neighborId: string }[] = requestBody.neighbors; // Access 'neighbors' array

    // Ensure that the country exists
    const country = await fetchCountryById(countryID as string);
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
    for (const neighborObj of neighborData) {
      const neighborId = neighborObj.neighborId;

      // Validate neighbor ID
      if (!existingCountryIds.includes(neighborId)) {
        errors.push(`Invalid neighbor country ID: ${neighborId}`);
        continue;
      }

      // Check if neighbor already exists for the country
      const existingNeighbor = await fetchNeighbor(countryID as string, neighborId);
      if (existingNeighbor) {
        errors.push(`Neighbor with ID ${neighborId} already exists for this country`);
        continue;
      }

      // Add neighbor to CountryNeighborsTable
      await addNeighbor(countryID as string, neighborId);
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

// Function to fetch all country IDs
const fetchAllCountryIds = async (): Promise<string[]> => {
  const result = await docClient
    .scan({
      TableName: tableName,
      ProjectionExpression: "countryID",
    })
    .promise();

  return result.Items ? result.Items.map((item) => item.countryID) : [];
};

// Function to fetch neighbor by country ID and neighbor ID
const fetchNeighbor = async (countryID: string, neighborId: string): Promise<any> => {
  const output = await docClient
    .get({
      TableName: nighbortableName,
      Key: {
        countryID: countryID,
        neighborId: neighborId,
      },
    })
    .promise();

  return output.Item;
};

// Function to add neighbor to CountryNeighborsTable
const addNeighbor = async (countryID: string, neighborId: string): Promise<void> => {
  await docClient
    .put({
      TableName: nighbortableName,
      Item: {
        countryID: countryID,
        neighborId: neighborId,
      },
    })
    .promise();
};

// Function to fetch country by ID
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

export const getCountryNeighbors = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const { countryID } = event.pathParameters || {};

    // Retrieve the country from the CountriesTable
    const country = await fetchCountryById(countryID as string);

    if (!country) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ message: "Country not found", data: {} }),
      };
    }

    // Retrieve neighbors from the CountryNeighborsTable
    const neighbors = await fetchNeighborsByCountryId(countryID as string);

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

// const fetchCountryById = async (id: string): Promise<any> => {
//   const output = await docClient
//     .get({
//       TableName: tableName,
//       Key: {
//         countryID: id,
//       },
//     })
//     .promise();

//   return output.Item;
// };

const fetchNeighborsByCountryId = async (countryID: string): Promise<string[]> => {
  const result = await docClient
    .query({
      TableName: nighbortableName,
      KeyConditionExpression: "countryID = :countryID",
      ExpressionAttributeValues: {
        ":countryID": countryID,
      },
    })
    .promise();

  return result.Items ? result.Items.map((item) => item.neighborId) : [];
};

export const getAllCountriesPaginated = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    let { page = "1", limit = "10", sort_by = "a_to_z", search } = event.queryStringParameters || {};
    let skip = (parseInt(page) - 1) * parseInt(limit);
    let sortKey: string;
    let sortDirection: number = 1;

    // Determine sorting criteria
    switch (sort_by) {
      case "a_to_z":
        sortKey = "name";
        sortDirection = 1;
        break;
      case "z_to_a":
        sortKey = "name";
        sortDirection = -1;
        break;
      case "population_high_to_low":
        sortKey = "population";
        sortDirection = -1;
        break;
      case "population_low_to_high":
        sortKey = "population";
        sortDirection = 1;
        break;
      case "area_high_to_low":
        sortKey = "area";
        sortDirection = -1;
        break;
      case "area_low_to_high":
        sortKey = "area";
        sortDirection = 1;
        break;
      default:
        sortKey = "name";
        sortDirection = 1;
        break;
    }

    // Set up query parameters
    let params: DynamoDB.DocumentClient.ScanInput = {
      TableName: tableName,
    };

    // Add search filter if provided
    if (search) {
      const searchRegex = new RegExp(search, "i");
      params.FilterExpression =
        "contains(#name, :search) OR contains(#region, :search) OR contains(#subregion, :search)";
      params.ExpressionAttributeNames = {
        "#name": "name",
        "#region": "region",
        "#subregion": "subregion",
      };
      params.ExpressionAttributeValues = {
        ":search": searchRegex.source, // Use source to extract regex pattern string
      };
    }

    // Perform the query
    const output: any = await docClient.scan(params).promise();

    // Sort the results
    if (sortKey) {
      output.Items.sort((a: any, b: any) => {
        const aValue = a[sortKey];
        const bValue = b[sortKey];
        return aValue < bValue ? -1 * sortDirection : aValue > bValue ? 1 * sortDirection : 0;
      });
    }

    // Paginate the results
    const totalCountries = output.Items.length;
    const totalPages = Math.ceil(totalCountries / parseInt(limit));
    const countries = output.Items.slice(skip, skip + parseInt(limit));

    // Prepare response
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
