plugins {
    id("com.android.application")
}

android {
    namespace = "com.job4me.midicapture"
    compileSdk = 37

    defaultConfig {
        applicationId = "com.job4me.midicapture"
        // getDevicesForTransport() (replacing the deprecated getDevices()) needs API 33+;
        // fine here since this targets a Fold 5, not old hardware.
        minSdk = 33
        targetSdk = 37
        versionCode = 1
        versionName = "0.1.0"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    buildFeatures {
        viewBinding = true
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.19.0")
    implementation("androidx.appcompat:appcompat:1.7.1")
    implementation("com.google.android.material:material:1.14.0")
}
