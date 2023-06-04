const os = require("os");
const childProcess = require("child_process");
const path = require("path");
const fs = require("fs");
const process = require("process");

// Get number of threads
const THREADS = os.cpus().length;
console.log(`Using ${THREADS} threads to build Kuzu.`);

// Install dependencies
console.log("Installing dependencies...");
childProcess.execSync("npm install", {
  cwd: path.join(__dirname, "kuzu-source", "tools", "nodejs_api"),
  stdio: "inherit",
});

// Build the Kuzu source code
console.log("Building Kuzu source code...");
const env = { ...process.env };

if (process.platform === "darwin") {
  const archflags = process.env["ARCHFLAGS"]
    ? archflags === "-arch arm64"
      ? "arm64"
      : archflags === "-arch x86_64"
      ? "x86_64"
      : null
    : null;
  if (archflags) {
    console.log(`The ARCHFLAGS is set to '${archflags}'.`);
    env["CMAKE_OSX_ARCHITECTURES"] = archflags;
  } else {
    console.log("The ARCHFLAGS is not set or is invalid and will be ignored.");
  }

  const deploymentTarget = process.env["MACOSX_DEPLOYMENT_TARGET"];
  if (deploymentTarget) {
    console.log(
      `The MACOSX_DEPLOYMENT_TARGET is set to '${deploymentTarget}'.`
    );
    env["CMAKE_OSX_DEPLOYMENT_TARGET"] = deploymentTarget;
  } else {
    console.log("The MACOSX_DEPLOYMENT_TARGET is not set and will be ignored.");
  }
}

childProcess.execSync("make nodejs NUM_THREADS=" + THREADS, {
  env,
  cwd: path.join(__dirname, "kuzu-source"),
  stdio: "inherit",
});

// Copy the built files to the package directory
const BUILT_DIR = path.join(
  __dirname,
  "kuzu-source",
  "tools",
  "nodejs_api",
  "build"
);
// Get all the js and node files
const files = fs.readdirSync(BUILT_DIR).filter((file) => {
  return file.endsWith(".js") || file.endsWith(".node");
});
console.log("Files to copy: ");
for (const file of files) {
  console.log("  " + file);
}
console.log("Copying built files to package directory...");
for (const file of files) {
  fs.copyFileSync(path.join(BUILT_DIR, file), path.join(__dirname, file));
}

// Remove the kuzu-source directory
console.log("Removing kuzu-source directory...");
fs.rmSync(path.join(__dirname, "kuzu-source"), { recursive: true });
console.log("Done!");
