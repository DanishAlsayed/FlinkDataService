package src.main.java;

import java.io.Serializable;

public enum RelationStructure implements Serializable {
    ROOT,
    LEAF,
    ONLY,
    INTERMEDIATE;

    public static RelationStructure getStructure(final boolean isRoot, final boolean isLeaf) {
        if (isRoot && isLeaf)
            return ONLY;
        if (isRoot && !isLeaf)
            return ROOT;
        if (!isRoot && isLeaf)
            return LEAF;
        return INTERMEDIATE;
    }
}
