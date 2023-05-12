# DataSanitizer

TL;DR: DataSanitizer - A lightweight, user-friendly Python tool that transforms sensitive database content into fake, coherent data to maintain privacy and compliance during development and testing.

DataSanitizer is a powerful and customizable Python script designed to desensitize sensitive data in databases, providing a secure and compliant solution for developers and testers. By replacing real data with realistic, yet fake information, DataSanitizer maintains the integrity of database relationships and logic while protecting the privacy of the original data. This ensures that developers can work with desensitized data, minimizing the risk of accidental data leaks and maintaining compliance with data protection regulations. DataSanitizer is highly configurable, allowing users to define which data columns need to be altered and the type of fake data to be generated for each column. With an easy-to-use interface and flexible options, DataSanitizer streamlines the process of data desensitization, enhancing the overall security and compliance of your development and testing workflows.

## Prerequisites

- Python 3.9 or higher
- Pip (Python Package Installer)

## Dependencies

Install the required dependencies by running:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project's root directory, containing your database credentials:

```sh
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_NAME=your_database_name
```

2. Configure the `config.json` file to specify the tables and columns to be desensitized, along with the data types for generating fake data. Here's an example:

```json
{
    "output_format": "csv",
    "tables": [
        {
            "name": "customers",
            "columns": [
                { "name": "customerName", "type": "company" },
                { "name": "contactLastName", "type": "last_name" },
                { "name": "contactFirstName", "type": "first_name" },
                { "name": "phone", "type": "phone" },
                { "name": "addressLine1", "type": "address" },
                { "name": "addressLine2", "type": "blank" },
                { "name": "city", "type": "city" },
                { "name": "state", "type": "state" },
                { "name": "postalCode", "type": "postal_code" },
                { "name": "country", "type": "country" },
                { "name": "creditLimit", "type": "float" }
            ]
        },
        {
            "name": "payments",
            "columns": [
                { "name": "paymentDate", "type": "date" },
                { "name": "checkNumber", "type": "check" },
                { "name": "amount", "type": "float" }
            ]
        }
    ]
}

```

## Usage

Run the script with the following command:

```sh
python desensitize.py
```

The desensitized data will be saved as CSV or SQL files (depending on your choice in the script) in a folder named after the database.

PS.: it is important to control the access to the config.json file which has the power to unmask data. In other words, only the data admin or the sys admin should be the one able to change the config.json file.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any improvements or bug fixes.

## License

This project is released under the [MIT License](LICENSE).
