package io.jenkins.plugins.dorametrics.store;

import hudson.Extension;
import hudson.model.AdministrativeMonitor;

/**
 * Tells administrators when the metrics database is on a network filesystem, where SQLite's
 * locking and syncing are unreliable and slow writes can delay builds.
 */
@Extension
public class NetworkStorageMonitor extends AdministrativeMonitor {

    @Override
    public String getDisplayName() {
        return "Pipeline DORA Metrics database on network storage";
    }

    @Override
    public boolean isActivated() {
        return getFileSystem() != null;
    }

    /** The network filesystem the database is on, or null. */
    public String getFileSystem() {
        try {
            return MetricsStore.getInstance().getNetworkFileSystem();
        } catch (RuntimeException e) {
            return null;
        }
    }

    public String getDatabasePath() {
        java.io.File file = MetricsStore.getInstance().getDatabaseFile();
        return file != null ? file.getPath() : "";
    }

    public String getDirectoryProperty() {
        return MetricsStore.DIRECTORY_PROPERTY;
    }
}
