export const getParcels = async () => {
  try {
    const result = await fetch(
      "https://hzrq7ak6j3.execute-api.eu-central-1.amazonaws.com/lab_2"

    );
    let data = await result.json();
    data = JSON.parse(data.body)
    return data;
  } catch (e) {
    console.log(e);
  }
};

