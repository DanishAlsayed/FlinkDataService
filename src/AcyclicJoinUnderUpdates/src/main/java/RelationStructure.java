package src.main.java;

public enum RelationStructure {
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
