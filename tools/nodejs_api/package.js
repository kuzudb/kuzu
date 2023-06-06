const childProcess = require("child_process");
const fs = require("fs/promises");
const fsCallback = require("fs");
const path = require("path");

const KUZU_ROOT = path.resolve(path.join(__dirname, "..", ".."));
const CURRENT_DIR = path.resolve(__dirname);
const ARCHIVE_PATH = path.resolve(path.join(__dirname, "kuzu-source.tar"));
const PREBUILT_DIR = path.join(CURRENT_DIR, "prebuilt");
const ARCHIVE_DIR_PATH = path.join(CURRENT_DIR, "package");
const KUZU_VERSION_TEXT = "Kuzu VERSION";

(async () => {
  console.log("Gathering Kuzu source code...");
  // Create the git archive
  await new Promise((resolve, reject) => {
    childProcess.execFile(
      "git",
      ["archive", "--format=tar", "--output=" + ARCHIVE_PATH, "HEAD"],
      {
        cwd: KUZU_ROOT,
      },
      (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      }
    );
  });

  // Remove the old kuzu-source directory
  try {
    await fs.rm(path.join(CURRENT_DIR, "kuzu-source"), { recursive: true });
  } catch (e) {
    // Ignore
  }

  // Create the kuzu-source directory
  await fs.mkdir(path.join(CURRENT_DIR, "kuzu-source"));

  // Extract the archive to kuzu-source
  await new Promise((resolve, reject) => {
    childProcess.execFile(
      "tar",
      ["-xf", ARCHIVE_PATH, "-C", "kuzu-source"],
      { cwd: CURRENT_DIR },
      (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      }
    );
  });

  // Remove the archive
  await fs.rm(ARCHIVE_PATH);

  // Remove the archive directory
  try {
    await fs.rm(ARCHIVE_DIR_PATH, { recursive: true });
  } catch (e) {
    // Ignore
  }

  // Create the archive directory
  await fs.mkdir(ARCHIVE_DIR_PATH);

  // Move kuzu-source to archive
  await fs.rename(
    path.join(CURRENT_DIR, "kuzu-source"),
    path.join(ARCHIVE_DIR_PATH, "kuzu-source")
  );

  // Copy package.json to archive
  await fs.copyFile(
    path.join(CURRENT_DIR, "package.json"),
    path.join(ARCHIVE_DIR_PATH, "package.json")
  );

  // Copy install.js to archive
  await fs.copyFile(
    path.join(CURRENT_DIR, "install.js"),
    path.join(ARCHIVE_DIR_PATH, "install.js")
  );

  // Copy LICENSE to archive
  await fs.copyFile(
    path.join(KUZU_ROOT, "LICENSE"),
    path.join(ARCHIVE_DIR_PATH, "LICENSE")
  );

  // Copy README.md to archive
  await fs.copyFile(
    path.join(KUZU_ROOT, "README.md"),
    path.join(ARCHIVE_DIR_PATH, "README.md")
  );

  // If prebuilt directory exists, copy the entire directory to archive
  const prebuiltDirExists = await new Promise((resolve, _) => {
    fsCallback.access(PREBUILT_DIR, fsCallback.constants.F_OK, (err) => {
      if (err) {
        resolve(false);
      } else {
        resolve(true);
      }
    });
  });

  if (prebuiltDirExists) {
    await fs.mkdir(path.join(ARCHIVE_DIR_PATH, "prebuilt"));
    console.log("Prebuilt directory exists, copying to archive...");
    const prebuiltFiles = await new Promise((resolve, _) => {
      fsCallback.readdir(PREBUILT_DIR, (err, files) => {
        if (err) {
          return resolve([]);
        }
        let prebuiltFiles = [];
        for (const file of files) {
          if (file.endsWith(".node")) {
            prebuiltFiles.push(file);
          }
        }
        resolve(prebuiltFiles);
      });
    });
    const copyPromises = [];
    for (const file of prebuiltFiles) {
      copyPromises.push(
        fs.copyFile(
          path.join(PREBUILT_DIR, file),
          path.join(ARCHIVE_DIR_PATH, "prebuilt", file)
        )
      );
    }
    await Promise.all(copyPromises);
    console.log(`Copied ${prebuiltFiles.length} files.`);
  } else {
    console.log("Prebuilt directory does not exist, skipping...");
  }

  console.log("Updating package.json...");

  const packageJson = JSON.parse(
    await fs.readFile(path.join(ARCHIVE_DIR_PATH, "package.json"), {
      encoding: "utf-8",
    })
  );

  const CMakeListsTxt = await fs.readFile(
    path.join(KUZU_ROOT, "CMakeLists.txt"),
    { encoding: "utf-8" }
  );

  // Get the version from CMakeLists.txt
  const lines = CMakeListsTxt.split("\n");
  for (const line of lines) {
    if (line.includes(KUZU_VERSION_TEXT)) {
      const version = line.split(" ")[2].trim();
      console.log("Found version string from CMakeLists.txt: " + version);
      packageJson.version = version;
      break;
    }
  }

  packageJson.scripts.install = "node install.js";
  // npm modifies environment variables during install, which causes build 
  // errors on Windows. This is a workaround.
  // packageJson.scripts.preinstall = "npm config set ignore-scripts true";
  // packageJson.scripts.postinstall = "npm config set ignore-scripts false";

  await fs.writeFile(
    path.join(ARCHIVE_DIR_PATH, "package.json"),
    JSON.stringify(packageJson, null, 2)
  );

  console.log("Creating tarball...");
  // Create the tarball
  await new Promise((resolve, reject) => {
    childProcess.execFile(
      "tar",
      ["-czf", "kuzu-source.tar.gz", "package"],
      { cwd: CURRENT_DIR },
      (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      }
    );
  });

  // Remove the archive directory
  console.log("Cleaning up...");
  try {
    await fs.rm(ARCHIVE_DIR_PATH, { recursive: true });
  } catch (e) {
    // Ignore
  }

  console.log("Done!");
})();
