# Burst client for Rust actions in Openwhisk

The purpose of this repository is give to user facilities to execute:  

- **Map** (classic approach): N functions in parallel to perform a map operation

- **Burst** (new approach): N functions in parallel that follow the burst paradigm and can communicate between them.

into the Openwhisk (burst version) platform.

For that, a series of explanations are documented above to clarify the usage of this client.

> ❗ If you are searching for more info about specific apps (terasort & kmeans), please visit the following READMEs:
>  * [Terasort classic](../ow_apps/terasort_classic/README.md)
>  * [Terasort burst](../ow_apps/terasort_burst/README.md)
>  * [Kmeans (only burst)](../ow_apps/kmeans/README.md)
> 
> For new implementations, please continue reading this README. 

## Configure the environment
Please access to the Python code in this repo and modify to convenience the Openwhisk IP, port, protocol..., in order to ensure that the client will connect correctly with Openwhisk controller.

## Precompile your code

To execute your experiments you can follow 2 approaches:

- Precompile the code in client side
- Let Openwhisk the responsability of compile the code

We strongly recommend to precompile the code (allowing Openwhisk won't waste time in compilation). To compile the code in your computer:

1. Access to your rust code directory. Ensure that this directory have the following structure: 

```bash
   |- Cargo.toml
   |- src
      |- lib.rs
```
Please visit [Runtime Rust README](https://github.com/apache/openwhisk-runtime-rust#managing-dependencies) to more info about structure.

2. Execute the next command:

```bash
 zip -r - * | docker run -i [docker_image] -debug -compile [function_name] > [output_file]
```
Example:

```bash
 zip -r - * | docker run -i openwhisk/action-rust-v1.34 -debug -compile main > main.zip
```
This command will compile your code inside the runtime needed (the logic is inside the runtime: communication middleware if needed, action_loop protocol, etc) and return a .zip file that contains an executable ready to post to Openwhisk.

## Execute your experiment
The `map` and `burst` functions perform the next actions:
 - Create action in Openwhisk
 - Invoke N actions in Openwhisk (either classic or burst) 
 - Continuous polling to Openwhisk checking the ending of functions
 - Print results at the end

Please use  `map` and `burst` functions to implement your logic. These two functions encapsulates the responsabilities to ease you the usage of Openwhisk. 
Navigate to implementation of these functions to more info (docstrings)

### Define the use case
Please define your use cases in `ow_apps/` folder. Create a dedicated folder for your use case and a nested `main.py` file to the logic of execution. 

Example: `ow_apps/kmeans/main.py`. 

For execute the script, please (in base directory of this repo) run: 

```bash
PYTHONPATH=. python3 ow_apps/<app_folder>/main.py
```

> ⚠️ This is a prototype. Please no doubt in modify code at convenience to make it work as expected.

