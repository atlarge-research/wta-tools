# asml-dev

## Usage

### Logging
It is important that when using the `core` module to build adapter layers, `Log4j2Configuration#setUpLoggingConfig`
is invoked early to set the user defined part of the logging configuration.
As of now, the user can define one part of the logging configuration using `core` logging, the log level. This
is set to `ERROR` by default, but the user can exert full control over this using the `config.json` file.

## Authors and Acknowledgement
### Developers 12A
- [Henry Page](https://gitlab.ewi.tudelft.nl/hpage)
- [Pil Kyu Cho](https://gitlab.ewi.tudelft.nl/pcho)
- [Lohithsai Yadala Chanchu](https://gitlab.ewi.tudelft.nl/lyadalachanchu)
- [Atour Mousavi Gourabi ](https://gitlab.ewi.tudelft.nl/amousavigourab)
- [Tianchen Qu](https://gitlab.ewi.tudelft.nl/tqu)
### Others
- **Teaching Assistant:** [Timur Oberhuber](https://gitlab.ewi.tudelft.nl/toberhuber)
- **Supervisor:** Johan Pouwelse
- **Client:** Laurens Versluis on behalf of ASML
## License
For open source projects, say how it is licensed.
